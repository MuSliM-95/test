from fastapi import APIRouter, Depends, HTTPException, Request
from datetime import datetime
import logging
import re

from api.chats.auth import get_current_user_for_avito as get_current_user
from api.chats.avito.schemas import (
    AvitoCredentialsCreate,
    AvitoWebhookResponse,
    AvitoSyncResponse,
    AvitoSendMessageRequest,
    AvitoSendMessageResponse,
    AvitoConnectResponse,
    AvitoChatsListResponse,
    AvitoChatListItem,
    AvitoMessagesResponse,
    AvitoMessageItem
)
from api.chats.avito.avito_handler import AvitoHandler
from api.chats.avito.avito_factory import (
    create_avito_client,
    validate_avito_credentials,
    save_token_callback,
    _encrypt_credential
)
from api.chats.avito.avito_webhook import process_avito_webhook
from api.chats import crud
from database.db import database, channels, channel_credentials, chats
from typing import List, Optional
from fastapi import Query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chats/avito", tags=["chats-avito"])


def extract_phone_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    
    phone_patterns = [
        r'\+?7\s?\(?\d{3}\)?\s?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
        r'8\s?\(?\d{3}\)?\s?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
        r'\+?7\d{10}',
        r'8\d{10}',
    ]
    
    for pattern in phone_patterns:
        matches = re.findall(pattern, text)
        if matches:
            phone = re.sub(r'[^\d+]', '', matches[0])
            if phone.startswith('8'):
                phone = '+7' + phone[1:]
            elif phone.startswith('7') and not phone.startswith('+7'):
                phone = '+' + phone
            elif len(phone) == 10:
                phone = '+7' + phone
            
            if phone.startswith('+7') and len(phone) == 12:
                return phone
            elif len(phone) >= 11:
                return phone
    
    return None


@router.get("/")
async def get_avito_api_info():
    return {
        "service": "Avito Messenger API Integration",
        "version": "1.0",
        "base_url": "/chats/avito",
        "endpoints": {
            "connect": {
                "method": "POST",
                "path": "/chats/avito/connect",
                "description": "Подключение канала Avito",
                "auth_required": True
            },
            "status": {
                "method": "GET",
                "path": "/chats/avito/status",
                "description": "Проверка статуса подключения",
                "auth_required": True
            },
            "chats": {
                "method": "GET",
                "path": "/chats/avito/chats",
                "description": "Получение списка чатов",
                "auth_required": True
            },
            "messages": {
                "method": "GET",
                "path": "/chats/avito/chats/{chat_id}/messages",
                "description": "Получение сообщений из чата",
                "auth_required": True
            },
            "send": {
                "method": "POST",
                "path": "/chats/avito/send",
                "description": "Отправка сообщения",
                "auth_required": True
            },
            "sync": {
                "method": "POST",
                "path": "/chats/avito/sync",
                "description": "Синхронизация всех сообщений",
                "auth_required": True
            },
            "webhook": {
                "method": "POST",
                "path": "/chats/avito/webhooks/message",
                "description": "Webhook для получения сообщений от Avito",
                "auth_required": False
            }
        },
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc",
            "openapi": "/openapi.json",
            "markdown": "See AVITO_API_DOCS.md file"
        },
        "authentication": {
            "methods": [
                "Query parameter: ?token=YOUR_TOKEN",
                "Header: Authorization: Bearer YOUR_TOKEN"
            ]
        }
    }


@router.post("/connect", response_model=AvitoConnectResponse)
async def connect_avito_channel(
    credentials: AvitoCredentialsCreate,
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        is_valid = await validate_avito_credentials(
            api_key=credentials.api_key,
            api_secret=credentials.api_secret,
            access_token=None  
        )
        
        if not is_valid:
            raise HTTPException(
                status_code=401,
                detail="Avito credentials validation failed - check your credentials"
            )
        
        from api.chats.avito.avito_client import AvitoClient
        temp_client = AvitoClient(
            api_key=credentials.api_key,
            api_secret=credentials.api_secret
        )
        token_data = await temp_client.get_access_token()
        
        access_token = token_data.get('access_token')
        refresh_token = token_data.get('refresh_token')
        expires_at_str = token_data.get('expires_at')
        
        from datetime import datetime as dt
        token_expires_at = dt.fromisoformat(expires_at_str) if expires_at_str else None
        
        if not access_token:
            raise HTTPException(
                status_code=500,
                detail="Failed to obtain access token from Avito API"
            )
        
        temp_client.access_token = access_token
        avito_user_id = None
        try:
            avito_user_id = await temp_client._get_user_id()
            logger.info(f"Retrieved Avito user_id: {avito_user_id}")
        except Exception as e:
            logger.warning(f"Could not retrieve user_id from Avito profile: {e}")
        
        avito_channel = await database.fetch_one(
            channels.select().where(channels.c.type == "AVITO")
        )
        
        if not avito_channel:
            channel_id = await database.execute(
                channels.insert().values(
                    name="Avito",
                    type="AVITO",
                    description="Avito White API Integration",
                    is_active=True
                )
            )
            avito_channel = await crud.get_channel(channel_id)
        
        channel_id = avito_channel['id']
        
        encrypted_api_key = _encrypt_credential(credentials.api_key)
        encrypted_api_secret = _encrypt_credential(credentials.api_secret)
        encrypted_access_token = _encrypt_credential(access_token)
        encrypted_refresh_token = _encrypt_credential(refresh_token) if refresh_token else None
        
        existing = await database.fetch_one(
            channel_credentials.select().where(
                (channel_credentials.c.channel_id == channel_id) &
                (channel_credentials.c.cashbox_id == cashbox_id) &
                (channel_credentials.c.is_active.is_(True))
            )
        )
        
        update_values = {
            "api_key": encrypted_api_key,
            "api_secret": encrypted_api_secret,
            "access_token": encrypted_access_token,
            "is_active": True,
            "updated_at": datetime.utcnow()
        }
        
        if encrypted_refresh_token:
            update_values["refresh_token"] = encrypted_refresh_token
        if token_expires_at:
            update_values["token_expires_at"] = token_expires_at
        if avito_user_id:
            update_values["avito_user_id"] = avito_user_id
        
        if existing:
            await database.execute(
                channel_credentials.update().where(
                    channel_credentials.c.id == existing['id']
                ).values(**update_values)
            )
            logger.info(f"Updated Avito credentials for channel={channel_id} cashbox={cashbox_id}")
        else:
            insert_values = {
                "channel_id": channel_id,
                "cashbox_id": cashbox_id,
                **update_values,
                "created_at": datetime.utcnow()
            }
            await database.execute(
                channel_credentials.insert().values(**insert_values)
            )
            logger.info(f"Inserted Avito credentials for channel={channel_id} cashbox={cashbox_id}")
        
        return {
            "success": True,
            "message": f"Avito канал успешно подключен к кабинету {cashbox_id}",
            "channel_id": channel_id,
            "cashbox_id": cashbox_id
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error connecting Avito channel: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Ошибка подключения: {str(e)}")


@router.get("/chats", response_model=AvitoChatsListResponse)
async def get_avito_chats(
    limit: int = Query(50, ge=1, le=100, description="Количество чатов для получения"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    unread_only: bool = Query(False, description="Только непрочитанные чаты"),
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channel = await database.fetch_one(
            channels.select().where(channels.c.type == "AVITO")
        )
        
        if not avito_channel:
            raise HTTPException(status_code=404, detail="Avito channel not configured")
        
        client = await create_avito_client(
            channel_id=avito_channel['id'],
            cashbox_id=cashbox_id,
            on_token_refresh=lambda token_data: save_token_callback(
                avito_channel['id'],
                cashbox_id,
                token_data
            )
        )
        
        if not client:
            raise HTTPException(
                status_code=500,
                detail="Could not create Avito API client. Check credentials."
            )
        
        avito_chats = await client.get_chats(
            limit=limit,
            offset=offset,
            unread_only=unread_only
        )
        
        created_count = 0
        updated_count = 0
        
        for avito_chat in avito_chats:
            try:
                external_chat_id = avito_chat.get('id')
                if not external_chat_id:
                    continue
                
                users = avito_chat.get('users', [])
                user_name = None
                user_phone = None
                
                if users:
                    first_user = users[0] if users else {}
                    user_name = first_user.get('name')
                    user_phone = (
                        first_user.get('phone') or
                        first_user.get('phone_number') or
                        first_user.get('public_user_profile', {}).get('phone') or
                        first_user.get('public_user_profile', {}).get('phone_number')
                    )
                
                if not user_phone:
                    last_message = avito_chat.get('last_message', {})
                    if last_message:
                        message_content = last_message.get('content', {})
                        message_text = None
                        if isinstance(message_content, dict):
                            message_text = message_content.get('text', '')
                        elif isinstance(message_content, str):
                            message_text = message_content
                        
                        if message_text and ('[Системное сообщение]' in message_text or 'системное' in message_text.lower()):
                            user_phone = extract_phone_from_text(message_text)
                            if user_phone:
                                logger.info(f"Extracted phone {user_phone} from system message in chat {external_chat_id}")
                
                existing_chat = await database.fetch_one(
                    chats.select().where(
                        (chats.c.channel_id == avito_channel['id']) &
                        (chats.c.external_chat_id == external_chat_id) &
                        (chats.c.cashbox_id == cashbox_id)
                    )
                )
                
                if existing_chat:
                    update_data = {
                        "updated_at": datetime.utcnow()
                    }
                    
                    if user_name:
                        update_data["name"] = user_name
                    if user_phone:
                        update_data["phone"] = user_phone
                    
                    last_message = avito_chat.get('last_message')
                    if last_message and last_message.get('created'):
                        from datetime import datetime as dt
                        last_message_time = dt.fromtimestamp(last_message['created'])
                        update_data["last_message_time"] = last_message_time
                    
                    await database.execute(
                        chats.update().where(
                            chats.c.id == existing_chat['id']
                        ).values(**update_data)
                    )
                    updated_count += 1
                else:
                    chat_data = {
                        "channel_id": avito_channel['id'],
                        "cashbox_id": cashbox_id,
                        "external_chat_id": external_chat_id,
                        "name": user_name,
                        "phone": user_phone,
                        "status": "ACTIVE",
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }
                    
                    if avito_chat.get('created'):
                        from datetime import datetime as dt
                        first_message_time = dt.fromtimestamp(avito_chat['created'])
                        chat_data["first_message_time"] = first_message_time
                    
                    last_message = avito_chat.get('last_message')
                    if last_message and last_message.get('created'):
                        from datetime import datetime as dt
                        last_message_time = dt.fromtimestamp(last_message['created'])
                        chat_data["last_message_time"] = last_message_time
                    
                    await database.execute(
                        chats.insert().values(**chat_data)
                    )
                    created_count += 1
                    logger.info(f"Created new chat in DB: external_chat_id={external_chat_id}")
            
            except Exception as e:
                logger.error(f"Failed to sync chat {avito_chat.get('id')}: {e}", exc_info=True)
        
        chat_items = [
            AvitoChatListItem(
                id=chat.get('id', ''),
                created=chat.get('created'),
                updated=chat.get('updated'),
                last_message=chat.get('last_message'),
                users=chat.get('users'),
                context=chat.get('context')
            )
            for chat in avito_chats
        ]
        
        return {
            "success": True,
            "total": len(avito_chats),
            "chats": chat_items,
            "created_in_db": created_count,
            "updated_in_db": updated_count
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting Avito chats: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error getting chats: {str(e)}")


@router.get("/chats/{chat_id}/messages", response_model=AvitoMessagesResponse)
async def get_avito_chat_messages(
    chat_id: str,
    limit: int = Query(50, ge=1, le=100, description="Количество сообщений для получения"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channel = await database.fetch_one(
            channels.select().where(channels.c.type == "AVITO")
        )
        
        if not avito_channel:
            raise HTTPException(status_code=400, detail="Avito channel not configured")
        
        chat = None
        try:
            internal_chat_id = int(chat_id)
            chat = await crud.get_chat(internal_chat_id)
        except ValueError:
            chat = await database.fetch_one(
                chats.select().where(
                    (chats.c.channel_id == avito_channel['id']) &
                    (chats.c.external_chat_id == chat_id) &
                    (chats.c.cashbox_id == cashbox_id)
                )
            )
        
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        if chat['cashbox_id'] != cashbox_id:
            raise HTTPException(status_code=403, detail="Access denied - chat belongs to another cashbox")
        
        if not chat.get('external_chat_id'):
            raise HTTPException(status_code=400, detail="Chat has no external_chat_id")
        
        if chat['channel_id'] != avito_channel['id']:
            raise HTTPException(status_code=400, detail="Chat is not from Avito channel")
        
        client = await create_avito_client(
            channel_id=avito_channel['id'],
            cashbox_id=cashbox_id,
            on_token_refresh=lambda token_data: save_token_callback(
                avito_channel['id'],
                cashbox_id,
                token_data
            )
        )
        
        if not client:
            raise HTTPException(
                status_code=500,
                detail="Could not create Avito API client. Check credentials."
            )
        
        avito_messages = await client.get_messages(
            chat_id=chat['external_chat_id'],
            limit=limit,
            offset=offset
        )
        
        saved_count = 0
        extracted_phone = None
        
        for avito_msg in avito_messages:
            msg_type = avito_msg.get('type', 'text')
            msg_content = avito_msg.get('content', {})
            msg_text = None
            
            if isinstance(msg_content, dict):
                msg_text = msg_content.get('text', '')
            elif isinstance(msg_content, str):
                msg_text = msg_content
            
            if msg_text and (msg_type == 'system' or '[Системное сообщение]' in msg_text or 'системное' in msg_text.lower()):
                phone = extract_phone_from_text(msg_text)
                if phone:
                    extracted_phone = phone
                    logger.info(f"Extracted phone {phone} from system message in chat {chat_id}")
                    break
        
        if extracted_phone and chat.get('phone') != extracted_phone:
            try:
                await database.execute(
                    chats.update().where(chats.c.id == chat['id']).values(phone=extracted_phone)
                )
                logger.info(f"Updated phone {extracted_phone} for chat {chat['id']}")
                chat['phone'] = extracted_phone
            except Exception as e:
                logger.warning(f"Failed to update phone in chat: {e}")
        
        for avito_msg in avito_messages:
            try:
                external_message_id = avito_msg.get('id')
                if not external_message_id:
                    continue
                
                from database.db import chat_messages
                existing_message = await database.fetch_one(
                    chat_messages.select().where(
                        chat_messages.c.external_message_id == external_message_id
                    )
                )
                
                if existing_message:
                    continue
                
                content = avito_msg.get('content', {})
                message_type_str = avito_msg.get('type', 'text')
                
                if isinstance(content, dict):
                    if message_type_str == 'text':
                        message_text = content.get('text', '')
                    elif message_type_str == 'link':
                        link_data = content.get('link', {})
                        message_text = link_data.get('text', link_data.get('url', '[Ссылка]'))
                    elif message_type_str == 'system':
                        message_text = content.get('text', '[Системное сообщение]')
                    elif message_type_str == 'image':
                        message_text = '[Изображение]'
                    elif message_type_str == 'item':
                        item_data = content.get('item', {})
                        message_text = f"Объявление: {item_data.get('title', '[Объявление]')}"
                    elif message_type_str == 'location':
                        loc_data = content.get('location', {})
                        message_text = loc_data.get('text', loc_data.get('title', '[Геолокация]'))
                    elif message_type_str == 'voice':
                        message_text = '[Голосовое сообщение]'
                    else:
                        message_text = f"[{message_type_str}]"
                else:
                    message_text = str(content) if content else f"[{message_type_str}]"
                
                direction = avito_msg.get('direction', 'in')
                sender_type = "CLIENT" if direction == "in" else "OPERATOR"
                
                message_type_str = avito_msg.get('type', 'text')
                message_type = AvitoHandler._map_message_type(message_type_str)
                
                is_read = avito_msg.get('is_read', False) or avito_msg.get('read') is not None
                status = "READ" if is_read else "DELIVERED"
                
                await crud.create_message_and_update_chat(
                    chat_id=chat['id'],
                    sender_type=sender_type,
                    content=message_text or f"[{message_type_str}]",
                    message_type=message_type,
                    external_message_id=external_message_id,
                    status=status
                )
                saved_count += 1
                logger.info(f"Saved message {external_message_id} to DB")
            
            except Exception as e:
                logger.error(f"Failed to save message {avito_msg.get('id')}: {e}", exc_info=True)
        
        message_items = [
            AvitoMessageItem(
                id=msg.get('id', ''),
                author_id=msg.get('author_id'),
                created=msg.get('created'),
                content=msg.get('content'),
                type=msg.get('type'),
                direction=msg.get('direction'),
                is_read=msg.get('is_read'),
                read=msg.get('read')
            )
            for msg in avito_messages
        ]
        
        return {
            "success": True,
            "chat_id": chat['id'],
            "external_chat_id": chat['external_chat_id'],
            "total": len(avito_messages),
            "messages": message_items,
            "saved_to_db": saved_count
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting Avito messages: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error getting messages: {str(e)}")


@router.post("/webhooks/message", response_model=AvitoWebhookResponse)
async def receive_avito_webhook(request: Request):
    try:
        body = await request.body()
        
        signature_header = request.headers.get("X-Avito-Signature")
        
        is_valid, webhook_data, cashbox_id = await process_avito_webhook(
            body,
            signature_header
        )
        
        if not is_valid:
            logger.error("Invalid webhook received")
            return {
                "success": False,
                "message": "Invalid webhook signature or structure"
            }
        
        if not cashbox_id:
            logger.error("Could not determine cashbox_id from webhook")
            return {
                "success": False,
                "message": "Could not determine cashbox_id"
            }
        
        try:
            from api.chats.avito.avito_types import AvitoWebhook
            webhook = AvitoWebhook(**webhook_data)
        except Exception as e:
            logger.error(f"Failed to parse webhook data into AvitoWebhook model: {e}")
            return {
                "success": False,
                "message": f"Invalid webhook structure: {str(e)}"
            }
        
        result = await AvitoHandler.handle_webhook_event(webhook, cashbox_id)
        
        return {
            "success": result.get("success", False),
            "message": result.get("message", "Event processed"),
            "chat_id": result.get("chat_id"),
            "message_id": result.get("message_id")
        }
    
    except Exception as e:
        logger.error(f"Error processing Avito webhook: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Error: {str(e)}"
        }


@router.post("/sync", response_model=AvitoSyncResponse)
async def sync_avito_messages(
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channel = await database.fetch_one(
            channels.select().where(channels.c.type == "AVITO")
        )
        
        if not avito_channel:
            raise HTTPException(status_code=404, detail="Avito channel not configured")
        
        client = await create_avito_client(
            channel_id=avito_channel['id'],
            cashbox_id=cashbox_id,
            on_token_refresh=lambda token_data: save_token_callback(
                avito_channel['id'],
                cashbox_id,
                token_data
            )
        )
        
        if not client:
            raise HTTPException(
                status_code=500,
                detail="Could not create Avito API client. Check credentials."
            )
        
        chats = await crud.get_chats(
            cashbox_id=cashbox_id,
            channel_id=avito_channel['id'],
            limit=100
        )
        
        synced_count = 0
        new_messages = 0
        updated_messages = 0
        errors = []
        
        for chat in chats:
            try:
                external_chat_id = chat.get('external_chat_id')
                if not external_chat_id:
                    logger.warning(f"Chat {chat['id']} has no external_chat_id")
                    continue

                avito_messages = await client.sync_messages(
                    chat_id=external_chat_id
                )
                
                logger.info(f"Synced {len(avito_messages)} messages from Avito chat {external_chat_id}")
                
                for avito_msg in avito_messages:
                    try:
                        external_message_id = avito_msg.get('id')
                        if not external_message_id:
                            continue
                        
                        from database.db import chat_messages
                        existing = await database.fetch_one(
                            chat_messages.select().where(
                                chat_messages.c.external_message_id == external_message_id
                            )
                        )
                        
                        if existing:
                            updated_messages += 1
                            continue
                        
                        content = avito_msg.get('content', {})
                        message_type_str = avito_msg.get('type', 'text')
                        message_text = ""
                        
                        if isinstance(content, dict):
                            if message_type_str == 'text':
                                message_text = content.get('text', '')
                            elif message_type_str == 'link':
                                link_data = content.get('link', {})
                                message_text = link_data.get('text', link_data.get('url', '[Ссылка]'))
                            elif message_type_str == 'system':
                                message_text = content.get('text', '[Системное сообщение]')
                            elif message_type_str == 'image':
                                message_text = '[Изображение]'
                            elif message_type_str == 'item':
                                item_data = content.get('item', {})
                                message_text = f"Объявление: {item_data.get('title', '[Объявление]')}"
                            elif message_type_str == 'location':
                                loc_data = content.get('location', {})
                                message_text = loc_data.get('text', loc_data.get('title', '[Геолокация]'))
                            elif message_type_str == 'voice':
                                message_text = '[Голосовое сообщение]'
                            else:
                                message_text = f"[{message_type_str}]"
                        else:
                            message_text = str(content) if content else f"[{message_type_str}]"
                        
                        direction = avito_msg.get('direction', 'in')
                        sender_type = "CLIENT" if direction == "in" else "OPERATOR"
                        
                        is_read = avito_msg.get('is_read', False) or avito_msg.get('read') is not None
                        status = "READ" if is_read else "DELIVERED"
                        
                        await crud.create_message_and_update_chat(
                            chat_id=chat['id'],
                            sender_type=sender_type,
                            content=message_text or f"[{message_type_str}]",
                            message_type=AvitoHandler._map_message_type(message_type_str),
                            external_message_id=external_message_id,
                            status=status
                        )
                        new_messages += 1
                    
                    except Exception as e:
                        logger.error(f"Failed to save message {avito_msg.get('id')}: {e}")
                        errors.append(f"Failed to save message: {str(e)}")
                
                synced_count += 1
            
            except Exception as e:
                logger.error(f"Failed to sync chat {chat['id']}: {e}")
                errors.append(f"Failed to sync chat {chat['id']}: {str(e)}")
        
        return {
            "synced_count": synced_count,
            "new_messages": new_messages,
            "updated_messages": updated_messages,
            "errors": errors
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Sync error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Sync error: {str(e)}")


@router.post("/send", response_model=AvitoSendMessageResponse)
async def send_avito_message(
    request: AvitoSendMessageRequest,
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        chat = await crud.get_chat(request.chat_id)
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        if chat['cashbox_id'] != cashbox_id:
            raise HTTPException(status_code=403, detail="Access denied - chat belongs to another cashbox")
        
        channel = await crud.get_channel(chat['channel_id'])
        if not channel or channel['type'] != 'AVITO':
            raise HTTPException(status_code=400, detail="Chat is not from Avito channel")
        
        message = await crud.create_message_and_update_chat(
            chat_id=request.chat_id,
            sender_type="OPERATOR",
            content=request.content,
            message_type=request.message_type or "TEXT",
            status="SENT"
        )
        
        external_message_id = None
        try:
            client = await create_avito_client(
                channel_id=channel['id'],
                cashbox_id=cashbox_id,
                on_token_refresh=lambda token_data: save_token_callback(
                    channel['id'],
                    cashbox_id,
                    token_data
                )
            )
            
            if not client:
                logger.warning(f"Could not create Avito client for channel {channel['id']}, cashbox {cashbox_id}")
                await crud.update_message(message['id'], status="FAILED")
                raise HTTPException(
                    status_code=500,
                    detail="Could not create Avito API client. Check credentials."
                )
            
            avito_message = await client.send_message(
                chat_id=chat['external_chat_id'],
                text=request.content
            )
            
            external_message_id = avito_message.get('id')
            
            await crud.update_message(
                message['id'],
                external_message_id=external_message_id,
                status="DELIVERED"
            )
            
            logger.info(f"Message {message['id']} sent to Avito, external_id: {external_message_id}")
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to send message to Avito: {e}", exc_info=True)
            try:
                await crud.update_message(message['id'], status="FAILED")
            except Exception as update_error:
                logger.error(f"Failed to update message status to FAILED: {update_error}")
            
            raise HTTPException(
                status_code=500,
                detail=f"Failed to send message to Avito API: {str(e)}"
            )
        
        return {
            "success": True,
            "message_id": str(message['id']),
            "external_message_id": external_message_id
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Send error: {str(e)}")


@router.get("/status")
async def get_avito_status(
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channel = await database.fetch_one(
            channels.select().where(channels.c.type == "AVITO")
        )
        
        if not avito_channel:
            return {
                "connected": False,
                "message": "Avito channel not configured in system"
            }
        
        credentials = await database.fetch_one(
            channel_credentials.select().where(
                (channel_credentials.c.channel_id == avito_channel['id']) &
                (channel_credentials.c.cashbox_id == cashbox_id) &
                (channel_credentials.c.is_active.is_(True))
            )
        )
        
        if not credentials:
            return {
                "connected": False,
                "channel_id": avito_channel['id'],
                "channel_name": avito_channel['name'],
                "cashbox_id": cashbox_id,
                "message": "Avito channel configured but no credentials for this cashbox"
            }
        
        try:
            client = await create_avito_client(
                channel_id=avito_channel['id'],
                cashbox_id=cashbox_id,
                on_token_refresh=lambda token_data: save_token_callback(
                    avito_channel['id'],
                    cashbox_id,
                    token_data
                )
            )
            
            if not client:
                return {
                    "connected": False,
                    "channel_id": avito_channel['id'],
                    "channel_name": avito_channel['name'],
                    "cashbox_id": cashbox_id,
                    "message": "Credentials are invalid or expired"
                }
            
            is_valid = await client.validate_token()
            
            return {
                "connected": is_valid,
                "channel_id": avito_channel['id'],
                "channel_name": avito_channel['name'],
                "cashbox_id": cashbox_id,
                "message": "Avito channel is connected" if is_valid else "Token validation failed"
            }
        
        except Exception as e:
            logger.warning(f"Failed to validate Avito token: {e}")
            return {
                "connected": False,
                "channel_id": avito_channel['id'],
                "channel_name": avito_channel['name'],
                "cashbox_id": cashbox_id,
                "message": f"Failed to validate connection: {str(e)}"
            }
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Status error: {str(e)}")

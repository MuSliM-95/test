from fastapi import APIRouter, Depends, HTTPException, Request
from datetime import datetime
import re
import os
import logging

logger = logging.getLogger(__name__)

from api.chats.auth import get_current_user_for_avito as get_current_user
from api.chats.avito.schemas import (
    AvitoCredentialsCreate,
    AvitoWebhookResponse,
    AvitoSyncResponse,
    AvitoConnectResponse,
    AvitoChatsListResponse,
    AvitoChatListItem,
    AvitoMessagesResponse,
    AvitoMessageItem,
    AvitoWebhookRegisterRequest,
    AvitoWebhookRegisterResponse,
    AvitoWebhookUpdateResponse
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
from typing import Optional
from fastapi import Query

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
            "sync": {
                "method": "POST",
                "path": "/chats/avito/sync",
                "description": "Синхронизация всех сообщений",
                "auth_required": True
            },
            "webhook_register": {
                "method": "POST",
                "path": "/chats/avito/webhooks/register",
                "description": "Регистрация webhook URL в Avito API",
                "auth_required": True
            },
            "webhooks_list": {
                "method": "GET",
                "path": "/chats/avito/webhooks/list",
                "description": "Получение списка зарегистрированных webhook'ов",
                "auth_required": True
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
        avito_account_name = None
        try:
            avito_user_id = await temp_client._get_user_id()
            user_profile = await temp_client.get_user_profile()
            avito_account_name = user_profile.get('name') or f"Cashbox {cashbox_id}"
        except Exception as e:
            avito_account_name = f"Cashbox {cashbox_id}"
        
        encrypted_api_key = _encrypt_credential(credentials.api_key)
        
        avito_channel = await crud.get_channel_by_cashbox_and_api_key(cashbox_id, encrypted_api_key, "AVITO")
        
        if not avito_channel:
            channel_name = f"Avito - {avito_account_name}" if avito_account_name else f"Avito - Cashbox {cashbox_id}"
            existing_channel = await database.fetch_one(
                channels.select().where(channels.c.name == channel_name)
            )
            if existing_channel:
                channel_name = f"Avito - {avito_account_name} ({cashbox_id})" if avito_account_name else f"Avito - Cashbox {cashbox_id}"
            
            channel_id = await database.execute(
                channels.insert().values(
                    name=channel_name,
                    type="AVITO",
                    description=f"Avito White API Integration for {avito_account_name or f'Cashbox {cashbox_id}'}",
                    is_active=True
                )
            )
            avito_channel = await crud.get_channel(channel_id)
        
        channel_id = avito_channel['id']
        
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
        
        webhook_registered = False
        webhook_error_message = None
        webhook_url = None
        try:
            webhook_url = "https://app.tablecrm.com/api/v1/avito/hook"
            
            if not webhook_url:
                webhook_error_message = "AVITO_DEFAULT_WEBHOOK_URL not set in .env file"
            else:
                client = await create_avito_client(
                    channel_id=channel_id,
                    cashbox_id=cashbox_id,
                    on_token_refresh=lambda token_data: save_token_callback(
                        channel_id,
                        cashbox_id,
                        token_data
                    )
                )
                if client:
                    try:
                        result = await client.register_webhook(webhook_url)
                        webhook_registered = True
                    except Exception as webhook_error:
                        webhook_error_message = str(webhook_error)
                else:
                    webhook_error_message = "Could not create Avito client"
        except Exception as e:
            webhook_error_message = str(e)
        
        response = {
            "success": True,
            "message": f"Avito канал успешно подключен к кабинету {cashbox_id}",
            "channel_id": channel_id,
            "cashbox_id": cashbox_id
        }
        
        if webhook_registered:
            response["webhook_registered"] = True
            response["webhook_url"] = webhook_url
        elif webhook_error_message:
            response["webhook_registered"] = False
            response["webhook_error"] = webhook_error_message
        
        return response
    
    except HTTPException:
        raise
    except Exception as e:
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
        
        avito_channels = await crud.get_all_channels_by_cashbox(cashbox_id, "AVITO")
        
        if not avito_channels:
            raise HTTPException(status_code=404, detail="Avito channel not configured for this cashbox")
        
        all_avito_chats = []
        created_count = 0
        updated_count = 0
        
        for avito_channel in avito_channels:
            try:
                client = await create_avito_client(
                    channel_id=avito_channel['id'],
                    cashbox_id=cashbox_id,
                    on_token_refresh=lambda token_data, ch_id=avito_channel['id']: save_token_callback(
                        ch_id,
                        cashbox_id,
                        token_data
                    )
                )
                
                if not client:
                    continue
                
                avito_chats = await client.get_chats(
                    limit=limit,
                    offset=offset,
                    unread_only=unread_only
                )
                
                all_avito_chats.extend(avito_chats)
                
                for avito_chat in avito_chats:
                    try:
                        external_chat_id = avito_chat.get('id')
                        if not external_chat_id:
                            continue
                        
                        users = avito_chat.get('users', [])
                        user_name = None
                        user_phone = None
                        
                        from database.db import channel_credentials
                        creds = await database.fetch_one(
                            channel_credentials.select().where(
                                (channel_credentials.c.channel_id == avito_channel['id']) &
                                (channel_credentials.c.cashbox_id == cashbox_id) &
                                (channel_credentials.c.is_active.is_(True))
                            )
                        )
                        avito_user_id = creds.get('avito_user_id') if creds else None
                        
                        if users and avito_user_id:
                            for user in users:
                                user_id_in_chat = user.get('user_id') or user.get('id')
                                if user_id_in_chat and user_id_in_chat != avito_user_id:
                                    user_name = user.get('name') or user.get('profile_name')
                                    user_phone = (
                                        user.get('phone') or
                                        user.get('phone_number') or
                                        user.get('public_user_profile', {}).get('phone') or
                                        user.get('public_user_profile', {}).get('phone_number')
                                    )
                                    if user_name or user_phone:
                                        break
                        
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
                        
                        existing_chat = await database.fetch_one(
                            chats.select().where(
                                (chats.c.channel_id == avito_channel['id']) &
                                (chats.c.external_chat_id == external_chat_id) &
                                (chats.c.cashbox_id == cashbox_id)
                            )
                        )
                        
                        if existing_chat:
                            try:
                                try:
                                    chat_info = await client.get_chat_info(external_chat_id)
                                except Exception as chat_error:
                                    error_str = str(chat_error)
                                    if "402" in error_str or "подписку" in error_str.lower() or "subscription" in error_str.lower():
                                        chat_info = {}
                                    else:
                                        raise
                                
                                users = chat_info.get('users', [])
                                
                                from database.db import channel_credentials
                                creds = await database.fetch_one(
                                    channel_credentials.select().where(
                                        (channel_credentials.c.channel_id == avito_channel['id']) &
                                        (channel_credentials.c.cashbox_id == cashbox_id) &
                                        (channel_credentials.c.is_active.is_(True))
                                    )
                                )
                                avito_user_id = creds.get('avito_user_id') if creds else None
                                
                                if users and avito_user_id:
                                    for user in users:
                                        user_id_in_chat = user.get('user_id') or user.get('id')
                                        if user_id_in_chat and user_id_in_chat != avito_user_id:
                                            user_name = user.get('name') or user.get('profile_name') or user_name
                                            user_phone_from_api = (
                                                user.get('phone') or
                                                user.get('phone_number') or
                                                user.get('public_user_profile', {}).get('phone') or
                                                user.get('public_user_profile', {}).get('phone_number')
                                            )
                                            if user_phone_from_api:
                                                user_phone = user_phone_from_api
                                            break
                                
                                if not user_phone:
                                    try:
                                        messages = await client.get_messages(external_chat_id, limit=50)
                                        for msg in messages:
                                            msg_content = msg.get('content', {})
                                            msg_text = msg_content.get('text', '') if isinstance(msg_content, dict) else str(msg_content)
                                            if msg_text:
                                                extracted_phone = extract_phone_from_text(msg_text)
                                                if extracted_phone:
                                                    user_phone = extracted_phone
                                                    break
                                    except Exception as e:
                                        error_str = str(e)
                                        if "402" in error_str or "подписку" in error_str.lower() or "subscription" in error_str.lower():
                                            pass
                                        else:
                                            pass
                            except Exception as e:
                                pass
                            
                            update_data = {}
                            
                            if user_name and existing_chat.get('name') != user_name:
                                update_data["name"] = user_name
                            if user_phone and existing_chat.get('phone') != user_phone:
                                update_data["phone"] = user_phone
                            
                            last_message = avito_chat.get('last_message')
                            if last_message and last_message.get('created'):
                                last_message_time = datetime.fromtimestamp(last_message['created'])
                                update_data["last_message_time"] = last_message_time
                            
                            if len(update_data) > 0:
                                if 'last_message_time' in update_data and update_data['last_message_time']:
                                    update_data['updated_at'] = update_data['last_message_time']
                                else:
                                    update_data['updated_at'] = datetime.utcnow()
                                
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
                            }
                            
                            if avito_chat.get('created'):
                                first_message_time = datetime.fromtimestamp(avito_chat['created'])
                                chat_data["first_message_time"] = first_message_time
                            
                            last_message = avito_chat.get('last_message')
                            if last_message and last_message.get('created'):
                                last_message_time = datetime.fromtimestamp(last_message['created'])
                                chat_data["last_message_time"] = last_message_time
                                chat_data["updated_at"] = last_message_time
                            else:
                                # Если нет last_message_time, используем текущее время
                                chat_data["updated_at"] = datetime.utcnow()
                            
                            await database.execute(
                                chats.insert().values(**chat_data)
                            )
                            created_count += 1
                    
                    except Exception as e:
                        logger.warning(f"Failed to sync chat: {e}")
            
            except Exception as e:
                continue
        
        chat_items = [
            AvitoChatListItem(
                id=chat.get('id', ''),
                created=chat.get('created'),
                updated=chat.get('updated'),
                last_message=chat.get('last_message'),
                users=chat.get('users'),
                context=chat.get('context')
            )
            for chat in all_avito_chats
        ]
        
        return {
            "success": True,
            "total": len(all_avito_chats),
            "chats": chat_items,
            "created_in_db": created_count,
            "updated_in_db": updated_count
        }
    
    except HTTPException:
        raise
    except Exception as e:
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
        
        chat = None
        try:
            internal_chat_id = int(chat_id)
            chat = await crud.get_chat(internal_chat_id)
        except ValueError:
            from database.db import channels
            from sqlalchemy import select, and_
            query = select([
                chats.c.id,
                chats.c.channel_id,
                chats.c.cashbox_id,
                chats.c.external_chat_id
            ]).select_from(
                chats.join(
                    channels,
                    chats.c.channel_id == channels.c.id
                )
            ).where(
                and_(
                    channels.c.type == 'AVITO',
                    channels.c.is_active.is_(True),
                    chats.c.external_chat_id == chat_id,
                    chats.c.cashbox_id == cashbox_id
                )
            ).limit(1)
            chat_result = await database.fetch_one(query)
            if chat_result:
                chat = await crud.get_chat(chat_result['id'])
        
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        if chat['cashbox_id'] != cashbox_id:
            raise HTTPException(status_code=403, detail="Access denied - chat belongs to another cashbox")
        
        if not chat.get('external_chat_id'):
            raise HTTPException(status_code=400, detail="Chat has no external_chat_id")
        
        avito_channel = await crud.get_channel(chat['channel_id'])
        if not avito_channel or avito_channel.get('type') != 'AVITO':
            raise HTTPException(status_code=400, detail="Chat is not from Avito channel")
        
        client = await create_avito_client(
            channel_id=chat['channel_id'],
            cashbox_id=cashbox_id,
            on_token_refresh=lambda token_data: save_token_callback(
                chat['channel_id'],
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
                    break
        
        if extracted_phone and chat.get('phone') != extracted_phone:
            try:
                from datetime import datetime
                await database.execute(
                    chats.update().where(chats.c.id == chat['id']).values(
                        phone=extracted_phone,
                        updated_at=datetime.utcnow()
                    )
                )
                chat['phone'] = extracted_phone
            except Exception as e:
                pass
        
        for avito_msg in avito_messages:
            try:
                external_message_id = avito_msg.get('id')
                if not external_message_id:
                    continue
                
                from database.db import chat_messages
                existing_message = await database.fetch_one(
                    chat_messages.select().where(
                        (chat_messages.c.external_message_id == external_message_id) &
                        (chat_messages.c.chat_id == chat['id'])
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
                        image_data = content.get('image', {})
                        if isinstance(image_data, dict):
                            sizes = image_data.get('sizes', {})
                            if isinstance(sizes, dict):
                                image_url = sizes.get('1280x960') or sizes.get('640x480') or (list(sizes.values())[0] if sizes else None)
                                message_text = f"[Image: {image_url if image_url else 'No URL'}]"
                            else:
                                message_text = "[Image message]"
                        else:
                            message_text = "[Image message]"
                    elif message_type_str == 'item':
                        item_data = content.get('item', {})
                        message_text = f"Объявление: {item_data.get('title', '[Объявление]')}"
                    elif message_type_str == 'location':
                        loc_data = content.get('location', {})
                        message_text = loc_data.get('text', loc_data.get('title', '[Геолокация]'))
                    elif message_type_str == 'voice':
                        voice_data = content.get('voice', {})
                        if isinstance(voice_data, dict):
                            duration = voice_data.get('duration')
                            voice_url = voice_data.get('url') or voice_data.get('voice_url')
                            voice_id = voice_data.get('voice_id')
                            if not voice_url and voice_id:
                                try:
                                    voice_url = await client.get_voice_file_url(voice_id)
                                except Exception as e:
                                    logger.warning(f"Failed to get voice URL for voice_id {voice_id}: {e}")
                            
                            if voice_url:
                                if duration and isinstance(duration, (int, float)):
                                    message_text = f"[Voice message: {duration}s - {voice_url}]"
                                else:
                                    message_text = f"[Voice message: {voice_url}]"
                            elif voice_id:
                                if duration and isinstance(duration, (int, float)):
                                    message_text = f"[Voice message: {duration}s - voice_id: {voice_id}]"
                                else:
                                    message_text = f"[Voice message: voice_id: {voice_id}]"
                            else:
                                if duration and isinstance(duration, (int, float)):
                                    message_text = f"[Voice message: {duration}s]"
                                else:
                                    message_text = "[Voice message]"
                        else:
                            message_text = "[Voice message]"
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
                
                created_timestamp = avito_msg.get('created')
                created_at = None
                if created_timestamp:
                    from datetime import datetime
                    created_at = datetime.fromtimestamp(created_timestamp)
                
                db_message = await crud.create_message_and_update_chat(
                    chat_id=chat['id'],
                    sender_type=sender_type,
                    content=message_text or f"[{message_type_str}]",
                    message_type=message_type,
                    external_message_id=external_message_id,
                    status=status,
                    created_at=created_at,
                    source="avito"
                )
                saved_count += 1
                    
                if message_type_str in ['image', 'voice'] and isinstance(content, dict):
                    try:
                        from database.db import pictures
                        file_url = None
                        
                        if message_type_str == 'image' and 'image' in content:
                            image_data = content['image']
                            sizes = image_data.get('sizes', {}) if isinstance(image_data, dict) else {}
                            if isinstance(sizes, dict):
                                file_url = sizes.get('1280x960') or sizes.get('640x480') or (list(sizes.values())[0] if sizes else None)
                        
                        elif message_type_str == 'voice' and 'voice' in content:
                            voice_data = content['voice']
                            if isinstance(voice_data, dict):
                                file_url = voice_data.get('url') or voice_data.get('voice_url')
                                if not file_url:
                                    voice_id = voice_data.get('voice_id')
                                    if voice_id:
                                        try:
                                            file_url = await client.get_voice_file_url(voice_id)
                                        except Exception as e:
                                            logger.warning(f"Failed to get voice URL for voice_id {voice_id}: {e}")
                        
                        if file_url:
                            await database.execute(
                                pictures.insert().values(
                                    entity="messages",
                                    entity_id=db_message['id'],
                                    url=file_url,
                                    is_main=False,
                                    is_deleted=False,
                                    owner=cashbox_id,
                                    cashbox=cashbox_id
                                )
                            )
                    except Exception as e:
                        logger.warning(f"Failed to save {message_type_str} file for message {external_message_id}: {e}")
            
            except Exception as e:
                logger.warning(f"Failed to save message {avito_msg.get('id')}: {e}")
        
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
        logger.error(f"Error getting Avito messages: {e}")
        raise HTTPException(status_code=400, detail=f"Error getting messages: {str(e)}")


@router.post("/sync", response_model=AvitoSyncResponse)
async def sync_avito_messages(
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channels = await crud.get_all_channels_by_cashbox(cashbox_id, "AVITO")
        
        if not avito_channels:
            raise HTTPException(status_code=404, detail="Avito channel not configured for this cashbox. Please connect via /connect endpoint first.")
        
        synced_count = 0
        new_messages = 0
        updated_messages = 0
        errors = []
        
        for avito_channel in avito_channels:
            try:
                client = await create_avito_client(
                    channel_id=avito_channel['id'],
                    cashbox_id=cashbox_id,
                    on_token_refresh=lambda token_data, ch_id=avito_channel['id']: save_token_callback(
                        ch_id,
                        cashbox_id,
                        token_data
                    )
                )
                
                if not client:
                    logger.warning(f"Could not create Avito API client for channel {avito_channel['id']}")
                    errors.append(f"Could not create client for channel {avito_channel['id']}")
                    continue
                
                chats = await crud.get_chats(
                    cashbox_id=cashbox_id,
                    channel_id=avito_channel['id'],
                    limit=100
                )
                
                for chat in chats:
                    try:
                        external_chat_id = chat.get('external_chat_id')
                        if not external_chat_id:
                            logger.warning(f"Chat {chat['id']} has no external_chat_id")
                            continue

                        try:
                            avito_messages = await client.sync_messages(
                                chat_id=external_chat_id
                            )
                        except Exception as sync_error:
                            error_str = str(sync_error)
                            if "402" in error_str or "подписку" in error_str.lower() or "subscription" in error_str.lower():
                                logger.info(f"Chat {chat['id']} (external: {external_chat_id}) requires subscription (402). Skipping.")
                                continue
                            raise
                        
                        for avito_msg in avito_messages:
                            try:
                                external_message_id = avito_msg.get('id')
                                if not external_message_id:
                                    continue
                                
                                from database.db import chat_messages
                                existing = await database.fetch_one(
                                    chat_messages.select().where(
                                        (chat_messages.c.external_message_id == external_message_id) &
                                        (chat_messages.c.chat_id == chat['id'])
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
                                
                                created_timestamp = avito_msg.get('created')
                                created_at = None
                                if created_timestamp:
                                    from datetime import datetime
                                    created_at = datetime.fromtimestamp(created_timestamp)
                                
                                await crud.create_message_and_update_chat(
                                    chat_id=chat['id'],
                                    sender_type=sender_type,
                                    content=message_text or f"[{message_type_str}]",
                                    message_type=AvitoHandler._map_message_type(message_type_str),
                                    external_message_id=external_message_id,
                                    status=status,
                                    source="avito",
                                    created_at=created_at
                                )
                                new_messages += 1
                            
                            except Exception as e:
                                logger.warning(f"Failed to save message {avito_msg.get('id')}: {e}")
                                errors.append(f"Failed to save message: {str(e)}")
                        
                        synced_count += 1
                    
                    except Exception as e:
                        logger.warning(f"Failed to sync chat {chat['id']}: {e}")
                        errors.append(f"Failed to sync chat {chat['id']}: {str(e)}")
            
            except Exception as e:
                logger.error(f"Failed to process channel {avito_channel['id']}: {e}")
                errors.append(f"Failed to process channel {avito_channel['id']}: {str(e)}")
                continue
        
        return {
            "synced_count": synced_count,
            "new_messages": new_messages,
            "updated_messages": updated_messages,
            "errors": errors
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Sync error: {e}")
        raise HTTPException(status_code=400, detail=f"Sync error: {str(e)}")


@router.get("/status")
async def get_avito_status(
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channel = await crud.get_channel_by_cashbox(cashbox_id, "AVITO")
        
        if not avito_channel:
            return {
                "connected": False,
                "message": "Avito channel not configured for this cashbox. Please connect via /connect endpoint first."
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
            logger.error(f"Failed to validate Avito token: {e}")
            return {
                "connected": False,
                "channel_id": avito_channel['id'],
                "channel_name": avito_channel['name'],
                "cashbox_id": cashbox_id,
                "message": f"Failed to validate connection: {str(e)}"
            }
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Status error: {str(e)}")


@router.post("/webhooks/register", response_model=AvitoWebhookRegisterResponse)
async def register_avito_webhook(
    request: AvitoWebhookRegisterRequest,
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channel = await crud.get_channel_by_cashbox(cashbox_id, "AVITO")
        
        if not avito_channel:
            raise HTTPException(status_code=400, detail="Avito channel not configured for this cashbox. Please connect via /connect endpoint first.")
        
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
        
        webhook_url = request.webhook_url
        if "{cashbox_id}" in webhook_url:
            webhook_url = webhook_url.replace("{cashbox_id}", str(cashbox_id))
        elif "/chat/" in webhook_url and webhook_url.count("/chat/") == 1:
            if not webhook_url.endswith(f"/{cashbox_id}"):
                webhook_url = f"{webhook_url.rstrip('/')}/{cashbox_id}"
        
        try:
            result = await client.register_webhook(webhook_url)
            
            
            return {
                "success": True,
                "message": "Webhook registered successfully",
                "webhook_url": webhook_url,
                "webhook_id": result.get('id') or result.get('webhook_id')
            }
        
        except Exception as e:
            logger.error(f"Failed to register webhook: {e}")
            raise HTTPException(
                status_code=400,
                detail=f"Failed to register webhook in Avito API: {str(e)}"
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering webhook: {e}")
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")


@router.get("/webhooks/list")
async def get_avito_webhooks(
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channel = await crud.get_channel_by_cashbox(cashbox_id, "AVITO")
        
        if not avito_channel:
            raise HTTPException(status_code=400, detail="Avito channel not configured for this cashbox. Please connect via /connect endpoint first.")
        
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
        
        webhooks = await client.get_webhooks()
        
        return {
            "success": True,
            "webhooks": webhooks,
            "count": len(webhooks)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting webhooks: {e}")
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")


@router.post("/webhooks/update-all", response_model=AvitoWebhookUpdateResponse)
async def update_all_avito_webhooks(
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channels = await crud.get_all_channels_by_cashbox(cashbox_id, "AVITO")
        
        if not avito_channels:
            return {
                "success": True,
                "message": "No Avito channels found for this cashbox",
                "updated_channels": 0,
                "failed_channels": 0,
                "results": []
            }
        
        webhook_url = "https://app.tablecrm.com/api/v1/avito/hook"
        
        if not webhook_url:
            return {
                "success": False,
                "message": "AVITO_DEFAULT_WEBHOOK_URL not set in .env file",
                "updated_channels": 0,
                "failed_channels": 0,
                "results": []
            }
        
        logger.info(f"Using webhook URL from .env for update-all: {webhook_url}")
        updated_count = 0
        failed_count = 0
        results = []
        
        for avito_channel in avito_channels:
            channel_id = avito_channel['id']
            channel_name = avito_channel.get('name', f'Channel {channel_id}')
            
            try:
                client = await create_avito_client(
                    channel_id=channel_id,
                    cashbox_id=cashbox_id,
                    on_token_refresh=lambda token_data, ch_id=channel_id: save_token_callback(
                        ch_id,
                        cashbox_id,
                        token_data
                    )
                )
                
                if not client:
                    failed_count += 1
                    results.append({
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "success": False,
                        "error": "Could not create Avito API client"
                    })
                    continue
                
                try:
                    await client.register_webhook(webhook_url)
                    updated_count += 1
                    results.append({
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "success": True,
                        "webhook_url": webhook_url
                    })
                    logger.info(f"Webhook updated for channel {channel_id} ({channel_name})")
                except Exception as webhook_error:
                    failed_count += 1
                    results.append({
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "success": False,
                        "error": str(webhook_error)
                    })
                    logger.warning(f"Failed to update webhook for channel {channel_id}: {webhook_error}")
                    
            except Exception as e:
                failed_count += 1
                results.append({
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "success": False,
                    "error": str(e)
                })
                logger.error(f"Error updating webhook for channel {channel_id}: {e}")
        
        return {
            "success": True,
            "message": f"Updated webhooks for {updated_count} channel(s), failed: {failed_count}",
            "updated_channels": updated_count,
            "failed_channels": failed_count,
            "results": results
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating webhooks: {e}")
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")

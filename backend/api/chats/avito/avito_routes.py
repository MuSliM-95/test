from fastapi import APIRouter, Depends, HTTPException, Request
from datetime import datetime
import re
import os
import logging
import urllib.parse
import secrets
import json

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
    AvitoWebhookUpdateResponse,
    AvitoOAuthAuthorizeResponse,
    AvitoOAuthCallbackResponse,
    AvitoHistoryLoadResponse,
    AvitoApplicantPhoneResponse,
    AvitoChatMetadataResponse
)
from api.chats.avito.avito_handler import AvitoHandler
from api.chats.avito.avito_factory import (
    create_avito_client,
    validate_avito_credentials,
    save_token_callback,
    _encrypt_credential,
    _decrypt_credential
)
from api.chats.avito.avito_webhook import process_avito_webhook
from api.chats.avito.avito_client import AvitoClient, AvitoAPIError
from api.chats import crud
from database.db import database, channels, channel_credentials, chats
from typing import Optional
from fastapi import Query, Request

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
            
            from api.chats.avito.avito_constants import AVITO_SVG_ICON
            channel_id = await database.execute(
                channels.insert().values(
                    name=channel_name,
                    type="AVITO",
                    svg_icon=AVITO_SVG_ICON,
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
                        user_avatar = None
                        client_user_id = None
                        
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
                                    client_user_id = user_id_in_chat
                                    user_name = user.get('name') or user.get('profile_name')
                                    user_phone = (
                                        user.get('phone') or
                                        user.get('phone_number') or
                                        user.get('public_user_profile', {}).get('phone') or
                                        user.get('public_user_profile', {}).get('phone_number')
                                    )
                                    public_profile = user.get('public_user_profile', {})
                                    if public_profile:
                                        avatar_data = public_profile.get('avatar', {})
                                        if isinstance(avatar_data, dict):
                                            user_avatar = (
                                                avatar_data.get('default') or
                                                avatar_data.get('images', {}).get('256x256') or
                                                avatar_data.get('images', {}).get('128x128') or
                                                (list(avatar_data.get('images', {}).values())[0] if avatar_data.get('images') else None)
                                            )
                                        elif isinstance(avatar_data, str):
                                            user_avatar = avatar_data
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
                            metadata = {}
                            context = avito_chat.get('context', {})
                            if isinstance(context, dict):
                                item = context.get('item', {})
                                if isinstance(item, dict):
                                    ad_title = item.get('title')
                                    ad_id = item.get('id')
                                    ad_url = item.get('url')
                                    if ad_title:
                                        metadata['ad_title'] = ad_title
                                    if ad_id:
                                        metadata['ad_id'] = ad_id
                                    if ad_url:
                                        metadata['ad_url'] = ad_url
                                if context:
                                    metadata['context'] = context
                            
                            if existing_chat.get('chat_contact_id'):
                                from database.db import chat_contacts
                                contact_update = {}
                                if user_name:
                                    contact_update['name'] = user_name
                                if user_phone:
                                    contact_update['phone'] = user_phone
                                if user_avatar:
                                    contact_update['avatar'] = user_avatar
                                
                                if client_user_id:
                                    existing_contact = await database.fetch_one(
                                        chat_contacts.select().where(chat_contacts.c.id == existing_chat['chat_contact_id'])
                                    )
                                    if existing_contact and not existing_contact.get('external_contact_id'):
                                        contact_update['external_contact_id'] = str(client_user_id)
                                
                                if contact_update:
                                    await database.execute(
                                        chat_contacts.update().where(
                                            chat_contacts.c.id == existing_chat['chat_contact_id']
                                        ).values(**contact_update)
                                    )
                            
                            chat_update = {}
                            if metadata:
                                chat_update['metadata'] = metadata
                            
                            last_message = avito_chat.get('last_message')
                            if last_message and last_message.get('created'):
                                last_message_time = datetime.fromtimestamp(last_message['created'])
                                chat_update['last_message_time'] = last_message_time
                                chat_update['updated_at'] = last_message_time
                            
                            if chat_update:
                                await database.execute(
                                    chats.update().where(
                                        chats.c.id == existing_chat['id']
                                    ).values(**chat_update)
                                )
                            
                            updated_count += 1
                        else:
                            metadata = {}
                            context = avito_chat.get('context', {})
                            if isinstance(context, dict):
                                item = context.get('item', {})
                                if isinstance(item, dict):
                                    ad_title = item.get('title')
                                    ad_id = item.get('id')
                                    ad_url = item.get('url')
                                    if ad_title:
                                        metadata['ad_title'] = ad_title
                                    if ad_id:
                                        metadata['ad_id'] = ad_id
                                    if ad_url:
                                        metadata['ad_url'] = ad_url
                                if context:
                                    metadata['context'] = context
                            
                            await crud.create_chat(
                                channel_id=avito_channel['id'],
                                cashbox_id=cashbox_id,
                                external_chat_id=external_chat_id,
                                external_chat_id_for_contact=str(client_user_id) if client_user_id else None,
                                name=user_name or (metadata.get('ad_title') if metadata else None) or f"Avito Chat {external_chat_id[:8]}",
                                phone=user_phone,
                                avatar=user_avatar,
                                metadata=metadata if metadata else None
                            )
                            created_count += 1
                    
                    except Exception as e:
                        logger.warning(f"Failed to sync chat: {e}")
            
            except Exception as e:
                continue
        
        chat_items = []
        for chat in all_avito_chats:
            last_message = chat.get('last_message')
            if last_message and isinstance(last_message, dict):
                last_message_clean = {k: v for k, v in last_message.items() if k != 'chat_id'}
                last_message_clean = {k: v for k, v in last_message_clean.items() if k != 'author_id'}
            else:
                last_message_clean = last_message
            
            context = chat.get('context')
            if context and isinstance(context, dict):
                context_clean = context.copy()
                if 'value' in context_clean and 'item' in context_clean:
                    value = context_clean.get('value', {})
                    item = context_clean.get('item', {})
                    if isinstance(value, dict) and isinstance(item, dict):
                        if value.get('id') == item.get('id'):
                            context_clean.pop('item', None)
            else:
                context_clean = context
            
            chat_items.append(
                AvitoChatListItem(
                    id=chat.get('id', ''),
                    created=chat.get('created'),
                    updated=chat.get('updated'),
                    last_message=last_message_clean,
                    users=chat.get('users'),
                    context=context_clean
                )
            )
        
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


@router.get("/chats/{chat_id}/applicant/phone", response_model=AvitoApplicantPhoneResponse)
async def get_avito_applicant_phone(
    chat_id: str,
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
        
        applicant_phone = None
        applicant_name = None
        
        if chat.get('chat_contact_id'):
            from database.db import chat_contacts
            contact = await database.fetch_one(
                chat_contacts.select().where(chat_contacts.c.id == chat['chat_contact_id'])
            )
            if contact:
                applicant_phone = contact.get('phone')
                applicant_name = contact.get('name')
        
        if not applicant_phone:
            client = await create_avito_client(
                channel_id=chat['channel_id'],
                cashbox_id=cashbox_id,
                on_token_refresh=lambda token_data: save_token_callback(
                    chat['channel_id'],
                    cashbox_id,
                    token_data
                )
            )
            
            if client:
                try:
                    external_chat_id = chat['external_chat_id']
                    chat_info = await client.get_chat_info(external_chat_id)
                    
                    users = chat_info.get('users', [])
                    
                    from database.db import channel_credentials as cc
                    creds = await database.fetch_one(
                        cc.select().where(
                            (cc.c.channel_id == chat['channel_id']) &
                            (cc.c.cashbox_id == cashbox_id) &
                            (cc.c.is_active.is_(True))
                        )
                    )
                    avito_user_id = creds.get('avito_user_id') if creds else None
                    
                    if users and avito_user_id:
                        for user in users:
                            user_id_in_chat = user.get('user_id') or user.get('id')
                            if user_id_in_chat and user_id_in_chat != avito_user_id:
                                applicant_phone = (
                                    user.get('phone') or
                                    user.get('phone_number') or
                                    user.get('public_user_profile', {}).get('phone') or
                                    user.get('public_user_profile', {}).get('phone_number')
                                )
                                if not applicant_name:
                                    applicant_name = user.get('name') or user.get('profile_name')
                                
                                if applicant_phone:
                                    break
                    
                    if not applicant_phone:
                        try:
                            messages = await client.get_messages(external_chat_id, limit=100)
                            for msg in messages:
                                msg_type = msg.get('type', 'text')
                                msg_content = msg.get('content', {})
                                msg_text = None
                                
                                if isinstance(msg_content, dict):
                                    msg_text = msg_content.get('text', '')
                                elif isinstance(msg_content, str):
                                    msg_text = msg_content
                                
                                if msg_text and (msg_type == 'system' or msg_text.startswith('[Системное сообщение]')):
                                    extracted_phone = extract_phone_from_text(msg_text)
                                    if extracted_phone:
                                        applicant_phone = extracted_phone
                                        logger.info(f"Found phone in system message: {applicant_phone}")
                                        break
                            
                            if not applicant_phone:
                                for msg in messages:
                                    msg_type = msg.get('type', 'text')
                                    msg_content = msg.get('content', {})
                                    msg_text = None
                                    
                                    if isinstance(msg_content, dict):
                                        msg_text = msg_content.get('text', '')
                                    elif isinstance(msg_content, str):
                                        msg_text = msg_content
                                    
                                    if msg_text and msg_type == 'text':
                                        extracted_phone = extract_phone_from_text(msg_text)
                                        if extracted_phone:
                                            applicant_phone = extracted_phone
                                            logger.info(f"Found phone in text message: {applicant_phone}")
                                            break
                        except Exception as e:
                            logger.warning(f"Could not extract phone from messages: {e}")
                            
                except Exception as e:
                    error_str = str(e)
                    if "402" in error_str or "подписку" in error_str.lower() or "subscription" in error_str.lower():
                        logger.info(f"Chat {chat['external_chat_id']} requires subscription (402) for getting applicant phone")
                    else:
                        logger.warning(f"Could not get applicant phone from Avito API: {e}")
        
        if applicant_phone and chat.get('chat_contact_id'):
            from database.db import chat_contacts
            current_contact = await database.fetch_one(
                chat_contacts.select().where(chat_contacts.c.id == chat['chat_contact_id'])
            )
            update_values = {"phone": applicant_phone}
            if applicant_name:
                update_values["name"] = applicant_name
            elif current_contact and not current_contact.get('name'):
                pass
            
            await database.execute(
                chat_contacts.update().where(
                    chat_contacts.c.id == chat['chat_contact_id']
                ).values(**update_values)
            )
        elif applicant_phone and not chat.get('chat_contact_id'):
            from database.db import chat_contacts
            contact_data = {
                "channel_id": chat['channel_id'],
                "name": applicant_name,
                "phone": applicant_phone
            }
            contact_result = await database.fetch_one(
                chat_contacts.insert().values(**contact_data).returning(chat_contacts.c.id)
            )
            if contact_result:
                await database.execute(
                    chats.update().where(chats.c.id == chat['id']).values(
                        chat_contact_id=contact_result['id']
                    )
                )
        
        return AvitoApplicantPhoneResponse(
            success=True,
            chat_id=chat['id'],
            phone=applicant_phone,
            name=applicant_name
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting applicant phone: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error getting applicant phone: {str(e)}")


@router.get("/chats/{chat_id}/metadata", response_model=AvitoChatMetadataResponse)
async def get_avito_chat_metadata(
    chat_id: str,
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
                chats.c.external_chat_id,
                chats.c.chat_contact_id
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
        
        metadata = {}
        
        chat_metadata = chat.get('metadata')
        if chat_metadata:
            if isinstance(chat_metadata, dict):
                metadata.update(chat_metadata)
            elif isinstance(chat_metadata, str):
                try:
                    import json
                    db_metadata = json.loads(chat_metadata)
                    if isinstance(db_metadata, dict):
                        metadata.update(db_metadata)
                except:
                    pass
        
        client = await create_avito_client(
            channel_id=chat['channel_id'],
            cashbox_id=cashbox_id,
            on_token_refresh=lambda token_data: save_token_callback(
                chat['channel_id'],
                cashbox_id,
                token_data
            )
        )
        
        if client:
            try:
                chat_info = await client.get_chat_info(chat['external_chat_id'])
                
                context = chat_info.get('context', {})
                if context:
                    metadata['context'] = context
                    
                    context_value = context.get('value') or context.get('item') or {}
                    
                    if context.get('value'):
                        context_value = context['value']
                        ad_title = context_value.get('title')
                        ad_id = context_value.get('id')
                        ad_url = context_value.get('url')
                        price_string = context_value.get('price_string')
                        status_id = context_value.get('status_id')
                        location = context_value.get('location', {})
                        location_title = location.get('title') if isinstance(location, dict) else None
                        images = context_value.get('images', {})
                        
                        if ad_title:
                            metadata['ad_title'] = ad_title
                            metadata['avito_title'] = ad_title
                        if ad_id:
                            metadata['ad_id'] = ad_id
                        if ad_url:
                            metadata['ad_url'] = ad_url
                            metadata['avito_ad'] = ad_url
                        if price_string:
                            metadata['price'] = price_string
                            metadata['price_string'] = price_string
                        if status_id is not None:
                            metadata['status_id'] = status_id
                        if location_title:
                            metadata['avito_location'] = location_title
                            metadata['location'] = location_title
                        if images:
                            metadata['images'] = images
                    
                    elif context.get('item'):
                        item = context['item']
                        ad_title = item.get('title')
                        ad_id = item.get('id')
                        ad_url = item.get('url')
                        
                        if ad_title:
                            metadata['ad_title'] = ad_title
                            metadata['avito_title'] = ad_title
                        if ad_id:
                            metadata['ad_id'] = ad_id
                        if ad_url:
                            metadata['ad_url'] = ad_url
                            metadata['avito_ad'] = ad_url
                
                if chat_info.get('created'):
                    metadata['avito_chat_created'] = chat_info.get('created')
                if chat_info.get('unread_count') is not None:
                    metadata['unread_count'] = chat_info.get('unread_count')
                if chat_info.get('messages_count') is not None:
                    metadata['messages_count'] = chat_info.get('messages_count')
                
                users = chat_info.get('users', [])
                if users:
                    metadata['users_count'] = len(users)
                    metadata['users'] = [
                        {
                            'user_id': user.get('user_id') or user.get('id'),
                            'name': user.get('name') or user.get('profile_name'),
                            'is_blocked': user.get('is_blocked', False)
                        }
                        for user in users
                    ]
                
            except Exception as e:
                error_str = str(e)
                if "402" in error_str or "подписку" in error_str.lower() or "subscription" in error_str.lower():
                    logger.info(f"Chat {chat['external_chat_id']} requires subscription (402) for getting metadata from Avito API")
                else:
                    logger.warning(f"Could not get metadata from Avito API: {e}")
        
        return AvitoChatMetadataResponse(
            success=True,
            chat_id=chat['id'],
            external_chat_id=chat['external_chat_id'],
            metadata=metadata
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting chat metadata: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error getting chat metadata: {str(e)}")


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
                    sort_by="updated_at",
                    sort_order="desc",
                    limit=1000  
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


@router.post("/history/load", response_model=AvitoHistoryLoadResponse)
async def load_avito_history(
    channel_id: int = Query(..., description="ID канала Avito"),
    from_date: int = Query(..., description="Unix timestamp, начиная с которого загружать историю"),
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channel = await crud.get_channel(channel_id)
        if not avito_channel:
            raise HTTPException(status_code=404, detail=f"Channel {channel_id} not found")
        
        if avito_channel.get('type') != 'AVITO':
            raise HTTPException(status_code=400, detail=f"Channel {channel_id} is not an Avito channel")
        
        channel_creds = await database.fetch_one(
            channel_credentials.select().where(
                (channel_credentials.c.channel_id == channel_id) &
                (channel_credentials.c.cashbox_id == cashbox_id) &
                (channel_credentials.c.is_active.is_(True))
            )
        )
        
        if not channel_creds:
            raise HTTPException(
                status_code=403,
                detail=f"Channel {channel_id} does not belong to cashbox {cashbox_id}"
            )
        
        client = await create_avito_client(
            channel_id=channel_id,
            cashbox_id=cashbox_id,
            on_token_refresh=lambda token_data: save_token_callback(
                channel_id,
                cashbox_id,
                token_data
            )
        )
        
        if not client:
            raise HTTPException(
                status_code=500,
                detail="Could not create Avito API client. Check credentials."
            )
        
        chats_processed = 0
        chats_created = 0
        chats_updated = 0
        messages_loaded = 0
        messages_created = 0
        messages_updated = 0
        errors = []
        
        all_chats = []
        offset = 0
        limit = 100
        max_offset = 1000
        
        logger.info(f"Loading Avito history for channel {channel_id} from timestamp {from_date}")
        
        while offset < max_offset:
            try:
                avito_chats = await client.get_chats(limit=limit, offset=offset)
                
                if not avito_chats:
                    break
                
                filtered_chats = [
                    chat for chat in avito_chats
                    if chat.get('created', 0) >= from_date
                ]
                
                all_chats.extend(filtered_chats)
                
                oldest_chat_time = min((chat.get('created', 0) for chat in avito_chats), default=0)
                if oldest_chat_time < from_date:
                    logger.info(f"Reached chats older than from_date at offset {offset}. Stopping pagination.")
                    break
                
                if len(avito_chats) < limit:
                    break
                
                offset += limit
                    
            except Exception as e:
                error_str = str(e)
                if "400" in error_str or offset >= 500:
                    logger.warning(f"Stopping pagination at offset {offset} due to error or large offset: {e}")
                    break
                if "402" in error_str or "подписку" in error_str.lower() or "subscription" in error_str.lower():
                    logger.info(f"Stopping pagination at offset {offset} due to subscription required (402)")
                    break
                logger.error(f"Error loading chats at offset {offset}: {e}")
                errors.append(f"Error loading chats at offset {offset}: {str(e)}")
                break
        
        logger.info(f"Found {len(all_chats)} chats created after {from_date}")
        
        for avito_chat in all_chats:
            try:
                external_chat_id = avito_chat.get('id')
                if not external_chat_id:
                    continue
                
                chats_processed += 1
                
                users = avito_chat.get('users', [])
                user_name = None
                user_phone = None
                user_avatar = None
                client_user_id = None
                
                from database.db import channel_credentials as cc
                creds = await database.fetch_one(
                    cc.select().where(
                        (cc.c.channel_id == channel_id) &
                        (cc.c.cashbox_id == cashbox_id) &
                        (cc.c.is_active.is_(True))
                    )
                )
                avito_user_id = creds.get('avito_user_id') if creds else None
                
                if users and avito_user_id:
                    for user in users:
                        user_id_in_chat = user.get('user_id') or user.get('id')
                        if user_id_in_chat and user_id_in_chat != avito_user_id:
                            client_user_id = user_id_in_chat
                            user_name = user.get('name') or user.get('profile_name')
                            user_phone = (
                                user.get('phone') or
                                user.get('phone_number') or
                                user.get('public_user_profile', {}).get('phone') or
                                user.get('public_user_profile', {}).get('phone_number')
                            )
                            public_profile = user.get('public_user_profile', {})
                            if public_profile:
                                avatar_data = public_profile.get('avatar', {})
                                if isinstance(avatar_data, dict):
                                    user_avatar = (
                                        avatar_data.get('default') or
                                        avatar_data.get('images', {}).get('256x256') or
                                        avatar_data.get('images', {}).get('128x128') or
                                        (list(avatar_data.get('images', {}).values())[0] if avatar_data.get('images') else None)
                                    )
                                elif isinstance(avatar_data, str):
                                    user_avatar = avatar_data
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
                
                from sqlalchemy import select
                existing_chat_query = select([
                    chats.c.id,
                    chats.c.channel_id,
                    chats.c.external_chat_id,
                    chats.c.cashbox_id,
                    chats.c.status,
                    chats.c.chat_contact_id,
                    chats.c.assigned_operator_id,
                    chats.c.first_message_time,
                    chats.c.last_message_time,
                    chats.c.created_at,
                    chats.c.updated_at
                ]).where(
                    (chats.c.channel_id == channel_id) &
                    (chats.c.external_chat_id == external_chat_id) &
                    (chats.c.cashbox_id == cashbox_id)
                )
                existing_chat = await database.fetch_one(existing_chat_query)
                
                if existing_chat:
                    metadata = {}
                    context = avito_chat.get('context', {})
                    if isinstance(context, dict):
                        item = context.get('item', {})
                        if isinstance(item, dict):
                            ad_title = item.get('title')
                            ad_id = item.get('id')
                            ad_url = item.get('url')
                            if ad_title:
                                metadata['ad_title'] = ad_title
                            if ad_id:
                                metadata['ad_id'] = ad_id
                            if ad_url:
                                metadata['ad_url'] = ad_url
                        if context:
                            metadata['context'] = context
                    
                    if existing_chat.get('chat_contact_id'):
                        from database.db import chat_contacts
                        contact_update = {}
                        existing_contact = None
                        
                        if user_name:
                            contact_update['name'] = user_name
                        if user_phone:
                            contact_update['phone'] = user_phone
                        if user_avatar:
                            contact_update['avatar'] = user_avatar

                        if client_user_id:
                            existing_contact = await database.fetch_one(
                                chat_contacts.select().where(chat_contacts.c.id == existing_chat['chat_contact_id'])
                            )
                            if existing_contact and (not existing_contact.get('external_contact_id') or existing_contact.get('external_contact_id') != str(client_user_id)):
                                contact_update['external_contact_id'] = str(client_user_id)
                        
                        if contact_update:
                            await database.execute(
                                chat_contacts.update().where(
                                    chat_contacts.c.id == existing_chat['chat_contact_id']
                                ).values(**contact_update)
                            )
                    elif (user_name or user_phone) and existing_chat['id']:
                        from database.db import chat_contacts
                        contact_data = {
                            "channel_id": channel_id,
                            "external_contact_id": str(client_user_id) if client_user_id else None,
                            "name": user_name,
                            "phone": user_phone,
                            "avatar": user_avatar
                        }
                        contact_result = await database.fetch_one(
                            chat_contacts.insert().values(**contact_data).returning(chat_contacts.c.id)
                        )
                        if contact_result:
                            await database.execute(
                                chats.update().where(
                                    chats.c.id == existing_chat['id']
                                ).values(chat_contact_id=contact_result['id'])
                            )
                    
                    chat_update = {}
                    if metadata:
                        chat_update['metadata'] = metadata
                    
                    last_message = avito_chat.get('last_message')
                    if last_message and last_message.get('created'):
                        last_message_time = datetime.fromtimestamp(last_message['created'])
                        chat_update['last_message_time'] = last_message_time
                        chat_update['updated_at'] = last_message_time
                    
                    if chat_update:
                        await database.execute(
                            chats.update().where(
                                chats.c.id == existing_chat['id']
                            ).values(**chat_update)
                        )
                    
                    chats_updated += 1
                    chat_id = existing_chat['id']
                else:
                    metadata = {}
                    context = avito_chat.get('context', {})
                    if isinstance(context, dict):
                        item = context.get('item', {})
                        if isinstance(item, dict):
                            ad_title = item.get('title')
                            ad_id = item.get('id')
                            ad_url = item.get('url')
                            if ad_title:
                                metadata['ad_title'] = ad_title
                            if ad_id:
                                metadata['ad_id'] = ad_id
                            if ad_url:
                                metadata['ad_url'] = ad_url
                        if context:
                            metadata['context'] = context
                    
                    chat_name = user_name or (metadata.get('ad_title') if metadata else None) or f"Avito Chat {external_chat_id[:8]}"
                    new_chat = await crud.create_chat(
                        channel_id=channel_id,
                        cashbox_id=cashbox_id,
                        external_chat_id=external_chat_id,
                        external_chat_id_for_contact=str(client_user_id) if client_user_id else None,
                        name=chat_name,
                        phone=user_phone,
                        avatar=user_avatar,
                        metadata=metadata if metadata else None
                    )
                    
                    if not new_chat or not new_chat.get('id'):
                        logger.error(f"Failed to create chat with external_id {external_chat_id}")
                        errors.append(f"Failed to create chat {external_chat_id}")
                        continue
                    
                    chat_id = new_chat['id']
                    chats_created += 1
                
                try:
                    avito_messages = await client.sync_messages(
                        chat_id=external_chat_id,
                        since_timestamp=from_date
                    )
                    messages_loaded += len(avito_messages)
                    
                    for avito_msg in avito_messages:
                        try:
                            external_message_id = avito_msg.get('id')
                            if not external_message_id:
                                continue
                            
                            from database.db import chat_messages
                            existing_message = await database.fetch_one(
                                chat_messages.select().where(
                                    (chat_messages.c.external_message_id == external_message_id) &
                                    (chat_messages.c.chat_id == chat_id)
                                )
                            )
                            
                            if existing_message:
                                messages_updated += 1
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
                                created_at = datetime.fromtimestamp(created_timestamp)
                            
                            await crud.create_message_and_update_chat(
                                chat_id=chat_id,
                                sender_type=sender_type,
                                content=message_text or f"[{message_type_str}]",
                                message_type=AvitoHandler._map_message_type(message_type_str),
                                external_message_id=external_message_id,
                                status=status,
                                source="avito",
                                created_at=created_at
                            )
                            messages_created += 1
                            
                        except Exception as e:
                            logger.warning(f"Failed to save message {avito_msg.get('id')}: {e}")
                            errors.append(f"Failed to save message {avito_msg.get('id')}: {str(e)}")
                    
                except Exception as sync_error:
                    error_str = str(sync_error)
                    if "402" in error_str or "подписку" in error_str.lower() or "subscription" in error_str.lower():
                        logger.info(f"Chat {external_chat_id} requires subscription (402). Skipping messages.")
                    else:
                        logger.warning(f"Failed to sync messages for chat {external_chat_id}: {sync_error}")
                        errors.append(f"Failed to sync messages for chat {external_chat_id}: {str(sync_error)}")
                
            except Exception as e:
                error_str = str(e)
                if "402" in error_str or "подписку" in error_str.lower() or "subscription" in error_str.lower():
                    logger.info(f"Chat {avito_chat.get('id')} requires subscription (402). Skipping.")
                else:
                    logger.warning(f"Failed to process chat {avito_chat.get('id')}: {e}")
                    errors.append(f"Failed to process chat {avito_chat.get('id')}: {str(e)}")
        
        logger.info(
            f"History load completed for channel {channel_id}: "
            f"chats_processed={chats_processed}, chats_created={chats_created}, "
            f"chats_updated={chats_updated}, messages_loaded={messages_loaded}, "
            f"messages_created={messages_created}, messages_updated={messages_updated}"
        )
        
        return AvitoHistoryLoadResponse(
            success=True,
            channel_id=channel_id,
            from_date=from_date,
            chats_processed=chats_processed,
            chats_created=chats_created,
            chats_updated=chats_updated,
            messages_loaded=messages_loaded,
            messages_created=messages_created,
            messages_updated=messages_updated,
            errors=errors if errors else None
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"History load error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"History load error: {str(e)}")


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
        
        logger.info(f"Using webhook URL for update-all: {webhook_url}")
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


@router.get("/oauth/authorize", response_model=AvitoOAuthAuthorizeResponse)
async def avito_oauth_authorize(
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channel = await crud.get_channel_by_cashbox(cashbox_id, "AVITO")
        if not avito_channel:
            raise HTTPException(
                status_code=404,
                detail=f"Avito channel not found for cashbox {cashbox_id}. Please connect Avito channel first via /connect endpoint."
            )
        
        channel_id = avito_channel['id']
        
        credentials = await database.fetch_one(
            channel_credentials.select().where(
                (channel_credentials.c.channel_id == channel_id) &
                (channel_credentials.c.cashbox_id == cashbox_id) &
                (channel_credentials.c.is_active.is_(True))
            )
        )
        
        if not credentials:
            raise HTTPException(
                status_code=404,
                detail=f"Avito credentials not found for cashbox {cashbox_id}. Please connect Avito channel first via /connect endpoint."
            )
        
        oauth_client_id = _decrypt_credential(credentials['api_key'])
        
        redirect_uri = "https://app.tablecrm.com/api/v1/hook/chat/123456"
        
        state = secrets.token_urlsafe(32)
        state_data = f"{cashbox_id}_{state}"
        
        scope = "messenger:read,messenger:write"
        oauth_params = {
            "client_id": oauth_client_id,
            "response_type": "code",
            "scope": scope,
            "state": state_data
        }
        
        encoded_params = urllib.parse.urlencode(oauth_params, safe=':,', quote_via=urllib.parse.quote)
        auth_url = "https://avito.ru/oauth" + "?" + encoded_params
        
        return AvitoOAuthAuthorizeResponse(
            authorization_url=auth_url,
            state=state_data,
            message="Перейдите по ссылке для авторизации в Avito"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")


@router.get("/oauth/callback", response_model=AvitoOAuthCallbackResponse)
async def avito_oauth_callback(
    code: str = Query(..., description="Authorization code from Avito"),
    state: str = Query(..., description="State parameter for CSRF protection"),
    token: Optional[str] = Query(None, description="Optional user authentication token")
):
    try:
        try:
            parts = state.split("_", 1)
            if len(parts) != 2:
                raise ValueError("State format invalid: expected cashbox_id_state_token")
            cashbox_id_str, state_token = parts
            cashbox_id = int(cashbox_id_str)
        except (ValueError, IndexError) as e:
            raise HTTPException(
                status_code=400,
                detail="Invalid state parameter"
            )
        
        avito_channel = await crud.get_channel_by_cashbox(cashbox_id, "AVITO")
        if not avito_channel:
            raise HTTPException(
                status_code=404,
                detail=f"Avito channel not found for cashbox {cashbox_id}. Please connect Avito channel first via /connect endpoint."
            )
        
        channel_id = avito_channel['id']
        
        credentials = await database.fetch_one(
            channel_credentials.select().where(
                (channel_credentials.c.channel_id == channel_id) &
                (channel_credentials.c.cashbox_id == cashbox_id) &
                (channel_credentials.c.is_active.is_(True))
            )
        )
        
        if not credentials:
            raise HTTPException(
                status_code=404,
                detail=f"Avito credentials not found for cashbox {cashbox_id}. Please connect Avito channel first via /connect endpoint."
            )
        
        oauth_client_id = _decrypt_credential(credentials['api_key'])
        oauth_client_secret = _decrypt_credential(credentials['api_secret'])
        
        redirect_uri = "https://app.tablecrm.com/api/v1/hook/chat/123456"
        
        try:
            token_data = await AvitoClient.exchange_authorization_code_for_tokens(
                client_id=oauth_client_id,
                client_secret=oauth_client_secret,
                authorization_code=code
            )
        except AvitoAPIError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to exchange authorization code: {str(e)}"
            )
        
        access_token = token_data.get('access_token')
        refresh_token = token_data.get('refresh_token')
        expires_at_str = token_data.get('expires_at')
        
        token_expires_at = datetime.fromisoformat(expires_at_str) if expires_at_str else None
        
        if not access_token:
            raise HTTPException(
                status_code=500,
                detail="Failed to obtain access token from Avito OAuth"
            )
        
        temp_client = AvitoClient(
            api_key=oauth_client_id,
            api_secret=oauth_client_secret,
            access_token=access_token,
            refresh_token=refresh_token,
            token_expires_at=token_expires_at
        )
        
        avito_user_id = None
        avito_account_name = None
        try:
            avito_user_id = await temp_client._get_user_id()
            user_profile = await temp_client.get_user_profile()
            avito_account_name = user_profile.get('name') or f"Cashbox {cashbox_id}"
        except Exception as e:
            avito_account_name = f"Cashbox {cashbox_id}"
        
        encrypted_access_token = _encrypt_credential(access_token)
        encrypted_refresh_token = _encrypt_credential(refresh_token) if refresh_token else None
        
        update_values = {
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
        
        await database.execute(
            channel_credentials.update().where(
                channel_credentials.c.id == credentials['id']
            ).values(**update_values)
        )
        
        webhook_registered = False
        webhook_error_message = None
        webhook_url = None
        try:
            webhook_url = "https://app.tablecrm.com/api/v1/avito/hook"
            
            if webhook_url:
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
        
        response_data = {
            "success": True,
            "message": f"Avito канал успешно подключен через OAuth к кабинету {cashbox_id}",
            "channel_id": channel_id,
            "cashbox_id": cashbox_id
        }
        
        if webhook_registered:
            response_data["webhook_registered"] = True
            response_data["webhook_url"] = webhook_url
        elif webhook_error_message:
            response_data["webhook_registered"] = False
            response_data["webhook_error"] = webhook_error_message
        
        return AvitoOAuthCallbackResponse(**response_data)
    
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")


@router.get("/hook", response_model=AvitoOAuthCallbackResponse)
async def avito_oauth_callback_hook(
    request: Request,
    code: str = Query(None, description="Authorization code from Avito"),
    state: str = Query(None, description="State parameter for CSRF protection"),
    error: str = Query(None, description="Error from Avito OAuth"),
    error_description: str = Query(None, description="Error description from Avito OAuth"),
    token: Optional[str] = Query(None, description="Optional user authentication token")
):
    if error:
        raise HTTPException(
            status_code=400,
            detail=f"OAuth error: {error}. {error_description or ''}"
        )
    
    if not code or not state:
        raise HTTPException(
            status_code=400,
            detail="Missing required OAuth parameters: code and state are required"
        )
    
    try:
        result = await avito_oauth_callback(code=code, state=state, token=token)
        return result
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Internal error during OAuth callback: {str(e)}"
        )


@router.get("/oauth/debug")
async def avito_oauth_debug(
    request: Request
):
    return {
        "url": str(request.url),
        "path": request.url.path,
        "query_params": dict(request.query_params),
        "headers": dict(request.headers),
        "message": "Это debug endpoint для проверки OAuth параметров"
    }

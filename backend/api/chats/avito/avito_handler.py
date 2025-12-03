import logging
import re
from typing import Optional, Dict, Any
from datetime import datetime
from .avito_types import AvitoWebhook
from ..producer import chat_producer
from .. import crud

logger = logging.getLogger(__name__)


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


class AvitoHandler:
    
    @staticmethod
    async def handle_message_event(webhook: AvitoWebhook, cashbox_id: int, channel_id: Optional[int] = None) -> Dict[str, Any]:
        try:
            payload = webhook.payload.value

            chat_id_external = payload.chat_id or ""
            author_id = payload.author_id
            user_id = payload.user_id
            message_id = payload.id or ""
            message_type = payload.type or 'text'
            
            message_content, message_text = AvitoHandler._extract_message_content(
                payload.content or {},
                message_type
            )
            
            existing_chat = None
            avito_channel = None
            if chat_id_external:
                from database.db import database, chats, channels
                from sqlalchemy import select, and_
                query = select([
                    chats.c.id,
                    chats.c.channel_id,
                    chats.c.cashbox_id
                ]).select_from(
                    chats.join(
                        channels,
                        chats.c.channel_id == channels.c.id
                    )
                ).where(
                    and_(
                        channels.c.type == 'AVITO',
                        channels.c.is_active.is_(True),
                        chats.c.external_chat_id == str(chat_id_external),
                        chats.c.cashbox_id == cashbox_id
                    )
                ).limit(1)
                existing_chat = await database.fetch_one(query)
                if existing_chat:
                    existing_channel_id = existing_chat['channel_id']
                    avito_channel = await crud.get_channel(existing_channel_id)
                    logger.info(f"Found existing chat {existing_chat['id']} with channel_id {existing_channel_id} for external_chat_id {chat_id_external}")
            
            # Если channel_id передан явно, используем его
            if channel_id:
                avito_channel = await crud.get_channel(channel_id)
                if avito_channel:
                    logger.info(f"Using provided channel_id {channel_id} for webhook processing")
            
            # Если чат не найден и channel_id не передан, пытаемся определить канал по user_id
            if not avito_channel and user_id:
                from database.db import channel_credentials
                from sqlalchemy import select, and_
                query = select([
                    channel_credentials.c.channel_id
                ]).select_from(
                    channel_credentials.join(
                        channels,
                        channel_credentials.c.channel_id == channels.c.id
                    )
                ).where(
                    and_(
                        channels.c.type == 'AVITO',
                        channels.c.is_active.is_(True),
                        channel_credentials.c.avito_user_id == user_id,
                        channel_credentials.c.cashbox_id == cashbox_id,
                        channel_credentials.c.is_active.is_(True)
                    )
                ).limit(1)
                creds_result = await database.fetch_one(query)
                if creds_result:
                    found_channel_id = creds_result['channel_id']
                    avito_channel = await crud.get_channel(found_channel_id)
                    logger.info(f"Found channel_id {found_channel_id} by avito_user_id {user_id} for cashbox {cashbox_id}")
            
            # Если все еще не найден, используем get_channel_by_cashbox (fallback)
            if not avito_channel:
                avito_channel = await crud.get_channel_by_cashbox(cashbox_id, "AVITO")
                if avito_channel:
                    logger.warning(f"Using fallback channel {avito_channel['id']} for cashbox {cashbox_id} (chat not found, user_id not matched)")
            
            if avito_channel:
                if not avito_channel.get('is_active', True):
                    return {"success": True, "message": "Event processed"}
                
                from database.db import channel_credentials
                creds = await database.fetch_one(
                    channel_credentials.select().where(
                        (channel_credentials.c.channel_id == avito_channel['id']) &
                        (channel_credentials.c.cashbox_id == cashbox_id)
                    )
                )
                if not creds or not creds.get('is_active', True):
                    return {"success": True, "message": "Event processed"}
            
            if message_type == 'voice' and isinstance(message_content, dict):
                voice_id = message_content.get('voice_id')
                if voice_id and not message_content.get('url') and not message_content.get('voice_url'):
                    try:
                        from api.chats.avito.avito_factory import create_avito_client, save_token_callback
                        if avito_channel:
                            voice_client = await create_avito_client(
                                channel_id=avito_channel['id'],
                                cashbox_id=cashbox_id,
                                on_token_refresh=lambda token_data: save_token_callback(
                                    avito_channel['id'],
                                    cashbox_id,
                                    token_data
                                )
                            )
                            if voice_client:
                                voice_url = await voice_client.get_voice_file_url(voice_id)
                                if voice_url:
                                    message_content['url'] = voice_url
                                    message_content['voice_url'] = voice_url
                                    duration = message_content.get('duration')
                                    if duration and isinstance(duration, (int, float)):
                                        message_text = f"[Voice message: {duration}s - {voice_url}]"
                                    else:
                                        message_text = f"[Voice message: {voice_url}]"
                    except Exception as e:
                        logger.warning(f"Failed to get voice URL for voice_id {voice_id} before message creation: {e}")
            
            sender_type = "CLIENT"
            avito_user_id = None
            try:
                from database.db import channel_credentials
                if avito_channel:
                    creds = await database.fetch_one(
                        channel_credentials.select().where(
                            (channel_credentials.c.channel_id == avito_channel['id']) &
                            (channel_credentials.c.cashbox_id == cashbox_id) &
                            (channel_credentials.c.is_active.is_(True))
                        )
                    )
                    if creds and creds.get('avito_user_id'):
                        avito_user_id = creds['avito_user_id']
                        if author_id and author_id == avito_user_id:
                            sender_type = "OPERATOR"
                        else:
                            sender_type = "CLIENT"
            except Exception as e:
                logger.warning(f"Could not determine message direction, defaulting to CLIENT: {e}")
            
            user_name = None
            user_phone = None
            context = None
            ad_title = None
            ad_id = None
            ad_url = None
            client_user_id = None  
            
            if sender_type == "CLIENT":
                if message_text:
                    if message_type == 'system' or ('[Системное сообщение]' in message_text or 'системное' in message_text.lower()):
                        extracted_phone = extract_phone_from_text(message_text)
                        if extracted_phone:
                            user_phone = extracted_phone
                    else:
                        extracted_phone = extract_phone_from_text(message_text)
                        if extracted_phone:
                            user_phone = extracted_phone
                
                try:
                    if not avito_channel:
                        logger.warning("Avito channel not found, cannot get chat info")
                    else:
                        from api.chats.avito.avito_factory import create_avito_client
                        from api.chats.avito.avito_factory import save_token_callback
                        client = await create_avito_client(
                            channel_id=avito_channel['id'],
                            cashbox_id=cashbox_id,
                            on_token_refresh=lambda token_data: save_token_callback(
                                avito_channel['id'],
                                cashbox_id,
                                token_data
                            )
                        )
                        if client:
                            chat_info = await client.get_chat_info(chat_id_external)
                            users = chat_info.get('users', [])
                            if users:
                                for user in users:
                                    user_id_in_chat = user.get('user_id') or user.get('id')
                                    if user_id_in_chat and user_id_in_chat != avito_user_id:
                                        client_user_id = str(user_id_in_chat)
                                        user_name = user.get('name') or user.get('profile_name')
                                        user_phone_from_api = (
                                            user.get('phone') or
                                            user.get('phone_number') or
                                            user.get('public_user_profile', {}).get('phone') or
                                            user.get('public_user_profile', {}).get('phone_number')
                                        )
                                        if user_phone_from_api:
                                            user_phone = user_phone_from_api
                                        if user_name or user_phone:
                                            break
                            
                            if not user_phone:
                                try:
                                    messages = await client.get_messages(chat_id_external, limit=50)
                                    for msg in messages:
                                        msg_content = msg.get('content', {})
                                        msg_text = msg_content.get('text', '') if isinstance(msg_content, dict) else str(msg_content)
                                        if msg_text:
                                            extracted_phone = extract_phone_from_text(msg_text)
                                            if extracted_phone:
                                                user_phone = extracted_phone
                                                break
                                except Exception as e:
                                    logger.warning(f"Could not get messages to extract phone: {e}")
                            else:
                                logger.warning(f"No users found in chat info for {chat_id_external}")

                            context = chat_info.get('context', {})
                            ad_title = None
                            ad_id = None
                            ad_url = None
                            if isinstance(context, dict):
                                item = context.get('item', {})
                                if isinstance(item, dict):
                                    ad_title = item.get('title')
                                    ad_id = item.get('id')
                                    ad_url = item.get('url')
                        else:
                            logger.warning("Could not create Avito client to get chat info")
                except Exception as e:
                    logger.error(f"Could not get chat info from Avito API: {e}", exc_info=True)
            
            if not user_name:
                if sender_type == "CLIENT":
                    user_name = f"Avito User {author_id}" if author_id else "Unknown User"
                else:
                    user_name = None
            
            if channel_id:
                target_channel = await crud.get_channel(channel_id)
                if target_channel:
                    existing_chat = await crud.get_chat_by_external_id(
                        channel_id=channel_id,
                        external_chat_id=chat_id_external,
                        cashbox_id=cashbox_id
                    )
                    if existing_chat:
                        chat = existing_chat
                    else:
                        # Извлекаем название объявления из payload, если есть
                        ad_title = None
                        metadata = {}
                        
                        if isinstance(payload, dict):
                            context = payload.get('context', {})
                            if isinstance(context, dict):
                                item = context.get('item', {})
                                if isinstance(item, dict):
                                    ad_title = item.get('title')
                                    ad_id = item.get('id')
                                    if ad_title:
                                        metadata['ad_title'] = ad_title
                                    if ad_id:
                                        metadata['ad_id'] = ad_id
                                if context:
                                    metadata['context'] = context
                        
                        # Используем название объявления как название чата
                        chat_name = ad_title or user_name or f"Avito Chat {chat_id_external[:8]}"
                        
                        chat = await crud.create_chat(
                            channel_id=channel_id,
                            cashbox_id=cashbox_id,
                            external_chat_id=chat_id_external,
                            external_chat_id_for_contact=client_user_id if client_user_id else None,
                            name=chat_name,
                            phone=user_phone,
                            metadata=metadata if metadata else None
                        )
                else:
                    chat = await AvitoHandler._find_or_create_chat(
                        channel_type="AVITO",
                        external_chat_id=chat_id_external,
                        cashbox_id=cashbox_id,
                        user_id=user_id or 0,
                        webhook_data=payload,
                        user_phone=user_phone,
                        user_name=user_name
                    )
            else:
                chat = await AvitoHandler._find_or_create_chat(
                    channel_type="AVITO",
                    external_chat_id=chat_id_external,
                    cashbox_id=cashbox_id,
                    user_id=user_id or 0,
                    webhook_data=payload,
                    user_phone=user_phone,
                    user_name=user_name
                )
            
            if not chat:
                raise Exception(f"Failed to create or find chat {chat_id_external}")
            
            chat_id = chat['id']
            
            if sender_type == "CLIENT":
                update_data = {}
                if user_name and chat.get('name') != user_name:
                    update_data['name'] = user_name
                if user_phone and chat.get('phone') != user_phone:
                    update_data['phone'] = user_phone
                
                current_metadata = chat.get('metadata') or {}
                if not isinstance(current_metadata, dict):
                    current_metadata = {}
                
                metadata_updated = False
                new_metadata = current_metadata.copy()
                
                if ad_title and not new_metadata.get('ad_title'):
                    new_metadata['ad_title'] = ad_title
                    metadata_updated = True
                if ad_id and not new_metadata.get('ad_id'):
                    new_metadata['ad_id'] = ad_id
                    metadata_updated = True
                if ad_url and not new_metadata.get('ad_url'):
                    new_metadata['ad_url'] = ad_url
                    metadata_updated = True
                if context and not new_metadata.get('context'):
                    new_metadata['context'] = context
                    metadata_updated = True
                
                if not chat.get('metadata') or metadata_updated:
                    update_data['metadata'] = new_metadata if new_metadata else None
                
                if not chat.get('name'):
                    if ad_title:
                        update_data['name'] = ad_title
                    elif user_name:
                        update_data['name'] = user_name
                
                if update_data:
                    try:
                        from database.db import database, chats, chat_contacts
                        from datetime import datetime
                        update_data['updated_at'] = datetime.utcnow()
                        await database.execute(
                            chats.update().where(chats.c.id == chat_id).values(**update_data)
                        )
                        chat.update(update_data)
                        
                        if chat.get('chat_contact_id'):
                            contact = await database.fetch_one(
                                chat_contacts.select().where(chat_contacts.c.id == chat['chat_contact_id'])
                            )
                            
                            if contact:
                                contact_dict = dict(contact) if contact else {}
                                contact_update = {}
                                if user_name and not contact_dict.get('name'):
                                    contact_update['name'] = user_name
                                if user_phone and not contact_dict.get('phone'):
                                    contact_update['phone'] = user_phone
                                if client_user_id and not contact_dict.get('external_contact_id'):
                                    contact_update['external_contact_id'] = str(client_user_id)
                                
                                if contact_update:
                                    contact_update['updated_at'] = datetime.utcnow()
                                    await database.execute(
                                        chat_contacts.update().where(
                                            chat_contacts.c.id == contact_dict['id']
                                        ).values(**contact_update)
                                    )
                    except Exception as e:
                        logger.warning(f"Failed to update chat info: {e}", exc_info=True)
            
            if client_user_id and chat.get('chat_contact_id'):
                try:
                    from database.db import database, chat_contacts
                    from datetime import datetime
                    contact = await database.fetch_one(
                        chat_contacts.select().where(chat_contacts.c.id == chat['chat_contact_id'])
                    )
                    if contact:
                        contact_dict = dict(contact) if contact else {}
                        if not contact_dict.get('external_contact_id') or contact_dict.get('external_contact_id') != str(client_user_id):
                            await database.execute(
                                chat_contacts.update().where(
                                    chat_contacts.c.id == contact_dict['id']
                                ).values(
                                    external_contact_id=str(client_user_id),
                                    updated_at=datetime.utcnow()
                                )
                            )
                            logger.info(f"Updated external_contact_id to {client_user_id} for contact {contact_dict['id']}")
                except Exception as e:
                    logger.warning(f"Failed to update external_contact_id: {e}", exc_info=True)
            
            from database.db import chat_messages
            existing_message = await database.fetch_one(
                chat_messages.select().where(
                    (chat_messages.c.external_message_id == message_id) &
                    (chat_messages.c.chat_id == chat_id)
                )
            )
            
            if existing_message:
                return {
                    "success": True,
                    "message": "Message already exists",
                    "chat_id": chat_id,
                    "message_id": existing_message['id']
                }
            
            created_at = None
            if hasattr(payload, 'created') and payload.created:
                from datetime import datetime
                created_at = datetime.fromtimestamp(payload.created)
            elif hasattr(payload, 'published_at') and payload.published_at:
                from datetime import datetime
                try:
                    created_at = datetime.fromisoformat(payload.published_at.replace('Z', '+00:00'))
                except Exception:
                    pass
            
            message = await crud.create_message_and_update_chat(
                chat_id=chat_id,
                sender_type=sender_type,
                content=message_text,
                message_type=AvitoHandler._map_message_type(message_type),
                external_message_id=message_id,
                status="DELIVERED",
                created_at=created_at,
                source="avito"
            )
            
            if message_type in ['image', 'voice'] and message_content:
                try:
                    from database.db import pictures, chats
                    file_url = None
                    
                    if message_type == 'image':
                        if isinstance(message_content, dict):
                            sizes = message_content.get('sizes', {})
                            if isinstance(sizes, dict):
                                file_url = sizes.get('1280x960') or sizes.get('640x480') or (list(sizes.values())[0] if sizes else None)
                            elif isinstance(sizes, str):
                                file_url = sizes
                    
                    elif message_type == 'voice':
                        if isinstance(message_content, dict):
                            file_url = message_content.get('url') or message_content.get('voice_url')
                            if not file_url:
                                voice_id = message_content.get('voice_id')
                                if voice_id:
                                    try:
                                        from api.chats.avito.avito_factory import create_avito_client, save_token_callback
                                        chat_data = await database.fetch_one(
                                            chats.select().where(chats.c.id == chat_id)
                                        )
                                        if chat_data:
                                            channel_id = chat_data['channel_id']
                                            avito_channel = await crud.get_channel(channel_id)
                                            if avito_channel and avito_channel.get('type') == 'AVITO':
                                                voice_client = await create_avito_client(
                                                    channel_id=channel_id,
                                                    cashbox_id=cashbox_id,
                                                    on_token_refresh=lambda token_data: save_token_callback(
                                                        channel_id,
                                                        cashbox_id,
                                                        token_data
                                                    )
                                                )
                                                if voice_client:
                                                    file_url = await voice_client.get_voice_file_url(voice_id)
                                    except Exception as e:
                                        logger.warning(f"Failed to get voice URL for voice_id {voice_id}: {e}")
                    
                    if file_url:
                        chat_data = await database.fetch_one(
                            chats.select().where(chats.c.id == chat_id)
                        )
                        cashbox_id_for_picture = chat_data['cashbox_id'] if chat_data else cashbox_id
                        
                        await database.execute(
                            pictures.insert().values(
                                entity="messages",
                                entity_id=message['id'],
                                url=file_url,
                                is_main=False,
                                is_deleted=False,
                                owner=cashbox_id_for_picture,
                                cashbox=cashbox_id_for_picture
                            )
                        )
                except Exception as e:
                    logger.warning(f"Failed to save {message_type} file for message {message['id']}: {e}")
            
            try:
                await chat_producer.send_message(chat_id, {
                    "message_id": message['id'],
                    "chat_id": chat_id,
                    "channel_type": "AVITO",
                    "external_message_id": message_id,
                    "sender_type": "CLIENT",
                    "content": message_text,
                    "message_type": message_type,
                    "created_at": datetime.utcnow().isoformat(),
                    "user_id": user_id,
                })
                logger.info(f"Sent message {message['id']} to RabbitMQ")
            except Exception as e:
                logger.error(f"Failed to send message to RabbitMQ: {e}")
            
            return {
                "success": True,
                "message": "Message processed successfully",
                "chat_id": chat_id,
                "message_id": message['id']
            }
            
        except Exception as e:
            logger.error(f"Error processing Avito webhook: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Failed to process message: {str(e)}",
                "error": str(e)
            }
    
    @staticmethod
    async def _find_or_create_chat(
        channel_type: str,
        external_chat_id: str,
        cashbox_id: int,
        user_id: int,
        webhook_data: Dict[str, Any],
        user_phone: Optional[str] = None,
        user_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        try:
            if channel_type == "AVITO":
                from database.db import database, channels, chats
                from sqlalchemy import select, and_
                
                existing_chat_query = select([
                    chats.c.id,
                    chats.c.channel_id,
                    chats.c.cashbox_id
                ]).select_from(
                    chats.join(
                        channels,
                        chats.c.channel_id == channels.c.id
                    )
                ).where(
                    and_(
                        channels.c.type == 'AVITO',
                        channels.c.is_active.is_(True),
                        chats.c.external_chat_id == str(external_chat_id),
                        chats.c.cashbox_id == cashbox_id
                    )
                ).limit(1)
                
                existing_chat_result = await database.fetch_one(existing_chat_query)
                
                if existing_chat_result:
                    channel_id = existing_chat_result['channel_id']
                    chat = await crud.get_chat(existing_chat_result['id'])
                    logger.info(f"Found existing chat {chat['id']} in channel {channel_id}")
                    return chat
                
                channel = None
                if user_id and user_id > 0:
                    from database.db import channel_credentials
                    query = select([
                        channel_credentials.c.channel_id
                    ]).select_from(
                        channel_credentials.join(
                            channels,
                            channel_credentials.c.channel_id == channels.c.id
                        )
                    ).where(
                        and_(
                            channels.c.type == 'AVITO',
                            channels.c.is_active.is_(True),
                            channel_credentials.c.avito_user_id == user_id,
                            channel_credentials.c.cashbox_id == cashbox_id,
                            channel_credentials.c.is_active.is_(True)
                        )
                    ).limit(1)
                    
                    creds_result = await database.fetch_one(query)
                    if creds_result:
                        channel_id = creds_result['channel_id']
                        channel = await crud.get_channel(channel_id)
                        logger.info(f"Found channel {channel_id} by avito_user_id {user_id} for cashbox {cashbox_id}")
                
                if not channel:
                    channel = await crud.get_channel_by_cashbox(cashbox_id, channel_type)
                    if channel:
                        logger.warning(f"Using fallback channel {channel['id']} for cashbox {cashbox_id} (user_id not matched)")
                
                if not channel:
                    logger.warning(f"Avito channel not found for cashbox {cashbox_id}. Channel should be created via /connect endpoint first.")
                    return None
            else:
                channel = await crud.get_channel_by_type(channel_type)
                if not channel:
                    logger.warning(f"Channel {channel_type} not found, creating new one")
                    channel = await crud.create_chat(
                        name=channel_type,
                        type=channel_type,
                        description=f"{channel_type} integration channel"
                    )
            
            existing_chat = await crud.get_chat_by_external_id(
                channel_id=channel['id'],
                external_chat_id=external_chat_id,
                cashbox_id=cashbox_id
            )
            
            if existing_chat:
                logger.info(f"Found existing chat: {existing_chat['id']}")
                return existing_chat
            ad_title = None
            metadata = {}
            client_user_id = None
            final_user_name = user_name
            final_user_phone = user_phone
            
            if channel_type == "AVITO" and channel:
                try:
                    from api.chats.avito.avito_factory import create_avito_client, save_token_callback
                    client = await create_avito_client(
                        channel_id=channel['id'],
                        cashbox_id=cashbox_id,
                        on_token_refresh=lambda token_data: save_token_callback(
                            channel['id'],
                            cashbox_id,
                            token_data
                        )
                    )
                    if client:
                        chat_info = await client.get_chat_info(external_chat_id)
                        if chat_info:
                            users = chat_info.get('users', [])
                            if users:
                                from database.db import channel_credentials
                                creds = await database.fetch_one(
                                    channel_credentials.select().where(
                                        (channel_credentials.c.channel_id == channel['id']) &
                                        (channel_credentials.c.cashbox_id == cashbox_id) &
                                        (channel_credentials.c.is_active.is_(True))
                                    )
                                )
                                avito_user_id = creds.get('avito_user_id') if creds else None
                                
                                for user in users:
                                    user_id_in_chat = user.get('user_id') or user.get('id')
                                    if user_id_in_chat and user_id_in_chat != avito_user_id:
                                        client_user_id = str(user_id_in_chat)
                                        if not final_user_name:
                                            final_user_name = user.get('name') or user.get('profile_name')
                                        if not final_user_phone:
                                            user_phone_from_api = (
                                                user.get('phone') or
                                                user.get('phone_number') or
                                                user.get('public_user_profile', {}).get('phone') or
                                                user.get('public_user_profile', {}).get('phone_number')
                                            )
                                            if user_phone_from_api:
                                                final_user_phone = user_phone_from_api
                                        if final_user_name or final_user_phone:
                                            break
                            
                            context = chat_info.get('context', {})
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
                except Exception as e:
                    logger.warning(f"Could not get chat info from API in _find_or_create_chat: {e}", exc_info=True)
            
            if not metadata and webhook_data:
                try:
                    if hasattr(webhook_data, '__dict__'):
                        webhook_dict = webhook_data.__dict__
                    elif isinstance(webhook_data, dict):
                        webhook_dict = webhook_data
                    else:
                        webhook_dict = {}
                    
                    context = webhook_dict.get('context', {})
                    if isinstance(context, dict):
                        item = context.get('item', {})
                        if isinstance(item, dict):
                            if not ad_title:
                                ad_title = item.get('title')
                            ad_id = item.get('id')
                            if ad_title and not metadata.get('ad_title'):
                                metadata['ad_title'] = ad_title
                            if ad_id and not metadata.get('ad_id'):
                                metadata['ad_id'] = ad_id
                        if context and not metadata.get('context'):
                            metadata['context'] = context
                except Exception as e:
                    logger.debug(f"Could not extract context from webhook_data: {e}")
            
            chat_name = ad_title or final_user_name or f"Avito Chat {external_chat_id[:8]}"
            
            logger.info(f"Creating new chat for Avito chat {external_chat_id} with name: {chat_name} in channel {channel['id']}")
            new_chat = await crud.create_chat(
                channel_id=channel['id'],
                cashbox_id=cashbox_id,
                external_chat_id=external_chat_id,
                external_chat_id_for_contact=str(client_user_id) if client_user_id else None,
                name=chat_name,
                phone=final_user_phone,
                metadata=metadata if metadata else None
            )
            
            return new_chat
            
        except Exception as e:
            logger.error(f"Failed to find/create chat: {e}", exc_info=True)
            return None
    
    @staticmethod
    def _extract_message_content(content: Dict[str, Any], message_type: str) -> tuple[Dict[str, Any], str]:
        if not content:
            return {}, ""
        
        message_content = content.copy() if isinstance(content, dict) else {}
        message_text = ""
        
        if message_type == 'text':
            message_text = content.get('text', '') if isinstance(content, dict) else str(content)
        
        elif message_type == 'image':
            if isinstance(content, dict) and 'image' in content:
                message_content = content['image']
                sizes = message_content.get('sizes', {}) if isinstance(message_content, dict) else {}
                if isinstance(sizes, dict):
                    image_url = sizes.get('1280x960') or sizes.get('640x480') or (list(sizes.values())[0] if sizes else None)
                    message_text = f"[Image: {image_url if image_url else 'No URL'}]"
                else:
                    message_text = "[Image message]"
            else:
                message_text = "[Image message]"
        
        elif message_type == 'voice':
            if isinstance(content, dict) and 'voice' in content:
                message_content = content['voice']
                duration = content['voice'].get('duration')
                voice_url = content['voice'].get('url') or content['voice'].get('voice_url')
                voice_id = content['voice'].get('voice_id')
                if voice_id and not voice_url:
                    message_content['voice_id'] = voice_id
                
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
        
        elif message_type == 'video':
            if isinstance(content, dict) and 'video' in content:
                message_content = content['video']
                message_text = f"[Video: {content['video'].get('url', 'No URL')}]"
            else:
                message_text = "[Video message]"
        
        elif message_type == 'file' or message_type == 'document':
            if isinstance(content, dict) and 'file' in content:
                message_content = content['file']
                message_text = f"[File: {content['file'].get('name', 'document')}]"
            else:
                message_text = "[Document]"
        
        elif message_type == 'link':
            if isinstance(content, dict) and 'link' in content:
                message_content = content['link']
                message_text = content['link'].get('text') or content['link'].get('url', '[Link]')
            else:
                message_text = "[Link]"
        
        elif message_type == 'location':
            if isinstance(content, dict) and 'location' in content:
                message_content = content['location']
                message_text = f"[Location: {content['location'].get('latitude')}, {content['location'].get('longitude')}]"
            else:
                message_text = "[Location]"
        
        elif message_type == 'item':
            if isinstance(content, dict) and 'item' in content:
                message_content = content['item']
                message_text = f"[Item: {content['item'].get('title', 'No title')}]"
            else:
                message_text = "[Avito item]"
        
        elif message_type == 'system':
            message_text = content.get('text', '[System message]') if isinstance(content, dict) else '[System message]'
        
        else:
            message_text = str(content) if not isinstance(content, dict) else "[Unknown message type]"
        
        return message_content, message_text
    
    @staticmethod
    def _map_message_type(avito_type: str) -> str:
        mapping = {
            'text': 'TEXT',
            'image': 'IMAGE',
            'voice': 'VOICE',
            'video': 'VIDEO',
            'file': 'DOCUMENT',
            'item': 'DOCUMENT', 
            'location': 'TEXT',  
            'link': 'TEXT',  
            'appCall': 'TEXT', 
            'system': 'SYSTEM',
        }
        return mapping.get(avito_type, 'TEXT')
    
    @staticmethod
    async def handle_webhook_event(
        webhook: AvitoWebhook,
        cashbox_id: int,
        channel_id: Optional[int] = None
    ) -> Dict[str, Any]:
        
        event_type = webhook.payload.type
        
        if event_type == 'message':
            return await AvitoHandler.handle_message_event(webhook, cashbox_id, channel_id)
        
        elif event_type == 'status':
            logger.info(f"Status event received (not implemented): {webhook.id}")
            return {"success": True, "message": "Status event received (not implemented)"}
        
        elif event_type == 'typing':
            logger.info(f"Typing event received (not implemented): {webhook.id}")
            return {"success": True, "message": "Typing event received"}
        
        else:
            logger.warning(f"Unknown event type: {event_type}")
            return {"success": False, "message": f"Unknown event type: {event_type}"}

    @staticmethod
    async def sync_messages_from_avito(
        client: "AvitoClient",
        chat_id: int,
        cashbox_id: int,
        external_chat_id: str
    ) -> Dict[str, Any]:
        try:
            logger.info(f"Starting sync for chat {chat_id} (external: {external_chat_id})")
            
            avito_messages = await client.sync_messages(external_chat_id)
            
            if not avito_messages:
                logger.info(f"No messages to sync for chat {chat_id}")
                return {
                    "success": True,
                    "synced_count": 0,
                    "new_messages": 0,
                    "updated_messages": 0,
                    "errors": []
                }
            
            synced_count = len(avito_messages)
            new_messages = 0
            updated_messages = 0
            errors = []
            
            for avito_msg in avito_messages:
                try:
                    message_id = avito_msg.get('id')
                    
                    existing_message = await crud.get_message_by_external_id(
                        chat_id=chat_id,
                        external_message_id=message_id
                    )
                    
                    if existing_message:
                        updated_messages += 1
                        continue
                    
                    message_text = avito_msg.get('content', {}).get('text', '') if avito_msg.get('content') else ''
                    message_type = avito_msg.get('type', 'text')
                    sender_id = avito_msg.get('authorId')
                    
                    sender_type = "CLIENT"
                    if sender_id and str(sender_id) == "0":
                        sender_type = "OPERATOR"
                    
                    message = await crud.create_message_and_update_chat(
                        chat_id=chat_id,
                        sender_type=sender_type,
                        content=message_text,
                        message_type=AvitoHandler._map_message_type(message_type),
                        external_message_id=message_id,
                        status="DELIVERED",
                        source="avito"
                    )
                    
                    logger.info(f"Synced message {message_id} to chat {chat_id}")
                    new_messages += 1
                    
                except Exception as msg_error:
                    logger.error(f"Error syncing message {avito_msg.get('id')}: {msg_error}")
                    errors.append({
                        "message_id": avito_msg.get('id'),
                        "error": str(msg_error)
                    })
            
            logger.info(f"Sync completed for chat {chat_id}: {new_messages} new, {updated_messages} updated")
            
            return {
                "success": True,
                "synced_count": synced_count,
                "new_messages": new_messages,
                "updated_messages": updated_messages,
                "errors": errors
            }
        
        except Exception as e:
            logger.error(f"Failed to sync messages for chat {chat_id}: {e}", exc_info=True)
            return {
                "success": False,
                "synced_count": 0,
                "new_messages": 0,
                "updated_messages": 0,
                "errors": [{"error": str(e)}]
            }

    @staticmethod
    async def handle_status_event(webhook: AvitoWebhook, cashbox_id: int) -> Dict[str, Any]:
        try:
            payload = webhook.payload.value
            
            message_id = payload.get('id')
            chat_id_external = payload.chat_id or ""
            status = payload.get('status')  
            
            logger.info(f"Status event for message {message_id} in chat {chat_id_external}: {status}")
            
            chat = await crud.get_chat_by_external_id(
                channel_id=None, 
                external_chat_id=chat_id_external,
                cashbox_id=cashbox_id
            )
            
            if not chat:
                logger.warning(f"Chat {chat_id_external} not found for status event")
                return {
                    "success": False,
                    "message": f"Chat {chat_id_external} not found"
                }
            
            message = await crud.get_message_by_external_id(
                chat_id=chat['id'],
                external_message_id=message_id
            )
            
            if not message:
                logger.warning(f"Message {message_id} not found in chat {chat['id']}")
                return {
                    "success": False,
                    "message": f"Message {message_id} not found"
                }
            
            if status == 'read':
                new_status = "READ"
            elif status == 'deleted':
                new_status = "DELETED"
            else:
                new_status = status.upper() if status else "UNKNOWN"
            
            await crud.update_message(message['id'], status=new_status)
            
            logger.info(f"Updated message {message['id']} status to {new_status}")
            
            return {
                "success": True,
                "message": f"Status updated to {new_status}",
                "message_id": message['id']
            }
        
        except Exception as e:
            logger.error(f"Error processing status event: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Error processing status event: {str(e)}"
            }

    @staticmethod
    async def handle_typing_event(webhook: AvitoWebhook, cashbox_id: int) -> Dict[str, Any]:
        try:
            payload = webhook.payload.value
            
            chat_id_external = payload.chat_id or ""
            is_typing = payload.get('isTyping', False)
            user_id = payload.get('authorId')
            
            logger.info(f"Typing event in chat {chat_id_external} from user {user_id}: {is_typing}")
            
            chat = await crud.get_chat_by_external_id(
                channel_id=None,
                external_chat_id=chat_id_external,
                cashbox_id=cashbox_id
            )
            
            if not chat:
                logger.warning(f"Chat {chat_id_external} not found for typing event")
                return {
                    "success": False,
                    "message": f"Chat {chat_id_external} not found"
                }
            
            logger.info(f"User {user_id} is {'typing' if is_typing else 'not typing'} in chat {chat['id']}")
            
            return {
                "success": True,
                "message": f"Typing event processed for chat {chat['id']}",
                "chat_id": chat['id'],
                "user_typing": is_typing
            }
        
        except Exception as e:
            logger.error(f"Error processing typing event: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Error processing typing event: {str(e)}"
            }

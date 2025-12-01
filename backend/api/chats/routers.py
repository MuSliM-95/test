from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
from datetime import datetime
from sqlalchemy import select

from api.chats import crud
from api.chats.auth import get_current_user, get_current_user_owner
from api.chats.schemas import (
    ChannelCreate, ChannelUpdate, ChannelResponse,
    ChatCreate, ChatUpdate, ChatResponse,
    MessageCreate, MessageResponse, MessagesList,
    ChainClientRequest,
    ManagersInChatResponse, ManagerInChat
)
from api.chats.websocket import chat_manager
from database.db import pictures, database

router = APIRouter(prefix="/chats", tags=["chats"])



@router.post("/channels/", response_model=ChannelResponse)
async def create_channel(token: str, channel: ChannelCreate, user = Depends(get_current_user_owner)):
    """Create a new channel (owner only)"""
    return await crud.create_channel(
        name=channel.name,
        type=channel.type,
        description=channel.description,
        svg_icon=channel.svg_icon,
        tags=channel.tags,
        api_config_name=channel.api_config_name
    )


@router.get("/channels/{channel_id}", response_model=ChannelResponse)
async def get_channel(channel_id: int, token: str, user = Depends(get_current_user)):
    channel = await crud.get_channel_by_id_and_cashbox(channel_id, user.cashbox_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found or access denied")
    return channel


@router.get("/channels/", response_model=list)
async def get_channels(token: str, skip: int = 0, limit: int = 100, user = Depends(get_current_user)):
    return await crud.get_all_channels_by_cashbox(user.cashbox_id)


@router.put("/channels/{channel_id}", response_model=ChannelResponse)
async def update_channel(channel_id: int, token: str, channel: ChannelUpdate, user = Depends(get_current_user_owner)):
    """Update channel (owner only)"""
    return await crud.update_channel(channel_id, **channel.dict(exclude_unset=True))


@router.delete("/channels/{channel_id}")
async def delete_channel(channel_id: int, token: str, user = Depends(get_current_user_owner)):
    """Delete channel (owner only, soft-delete)"""
    return await crud.delete_channel(channel_id)


@router.post("/chats/", response_model=ChatResponse)
async def create_chat(token: str, chat: ChatCreate, user = Depends(get_current_user)):
    """Create a new chat (cashbox_id from token)"""
    return await crud.create_chat(
        channel_id=chat.channel_id,
        cashbox_id=user.cashbox_id,
        external_chat_id=chat.external_chat_id,
        assigned_operator_id=chat.assigned_operator_id,
        external_chat_id_for_contact=chat.external_chat_id,
        phone=chat.phone,
        name=chat.name
    )


@router.get("/chats/{chat_id}", response_model=ChatResponse)
async def get_chat(chat_id: int, token: str, user = Depends(get_current_user)):
    """Get chat by ID (must belong to user's cashbox)"""
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    if chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return chat


@router.get("/chats/", response_model=list)
async def get_chats(
    token: str,
    channel_id: Optional[int] = None,
    contragent_id: Optional[int] = None,
    status: Optional[str] = None,
    search: Optional[str] = Query(None, description="Поиск по имени, телефону или external_chat_id"),
    created_from: Optional[datetime] = Query(None, description="Фильтр: дата создания от (ISO 8601)"),
    created_to: Optional[datetime] = Query(None, description="Фильтр: дата создания до (ISO 8601)"),
    updated_from: Optional[datetime] = Query(None, description="Фильтр: дата обновления от (ISO 8601)"),
    updated_to: Optional[datetime] = Query(None, description="Фильтр: дата обновления до (ISO 8601)"),
    sort_by: Optional[str] = Query(None, description="Сортировка по полю: created_at, updated_at, last_message_time, name"),
    sort_order: Optional[str] = Query("desc", description="Порядок сортировки: asc или desc"),
    skip: int = 0,
    limit: int = 100,
    user = Depends(get_current_user)
):
    return await crud.get_chats(
        cashbox_id=user.cashbox_id,
        channel_id=channel_id,
        contragent_id=contragent_id,
        status=status,
        search=search,
        created_from=created_from,
        created_to=created_to,
        updated_from=updated_from,
        updated_to=updated_to,
        sort_by=sort_by,
        sort_order=sort_order,
        skip=skip,
        limit=limit
    )


@router.put("/chats/{chat_id}", response_model=ChatResponse)
async def update_chat(chat_id: int, token: str, chat: ChatUpdate, user = Depends(get_current_user)):
    """Update chat (must belong to user's cashbox)"""
    existing_chat = await crud.get_chat(chat_id)
    if not existing_chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    if existing_chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return await crud.update_chat(chat_id, **chat.dict(exclude_unset=True))


@router.delete("/chats/{chat_id}")
async def delete_chat(chat_id: int, token: str, user = Depends(get_current_user)):
    import logging
    logger = logging.getLogger(__name__)
    
    existing_chat = await crud.get_chat(chat_id)
    if not existing_chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    if existing_chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    if existing_chat.get('external_chat_id'):
        try:
            channel = await crud.get_channel(existing_chat['channel_id'])
            
            if channel and channel['type'] == 'AVITO':
                from api.chats.avito.avito_factory import create_avito_client, save_token_callback
                
                client = await create_avito_client(
                    channel_id=channel['id'],
                    cashbox_id=user.cashbox_id,
                    on_token_refresh=lambda token_data: save_token_callback(
                        channel['id'],
                        user.cashbox_id,
                        token_data
                    )
                )
                
                if client:
                    try:
                        closed = await client.close_chat(existing_chat['external_chat_id'])
                        if closed:
                            logger.info(f"Chat {chat_id} closed in Avito API")
                        else:
                            logger.warning(f"Failed to close chat {chat_id} in Avito API")
                    except Exception as e:
                        logger.warning(f"Error closing chat in Avito API: {e}")
        except Exception as e:
            logger.warning(f"Error during Avito chat closure: {e}")
    
    return await crud.update_chat(chat_id, status="CLOSED")


@router.post("/messages/", response_model=MessageResponse)
async def create_message(token: str, message: MessageCreate, user = Depends(get_current_user)):
    """Create a new message"""
    import logging
    logger = logging.getLogger(__name__)
    
    chat = await crud.get_chat(message.chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db_message = await crud.create_message_and_update_chat(
        chat_id=message.chat_id,
        sender_type=message.sender_type,
        content=message.content,
        message_type=message.message_type,
        external_message_id=None,  
        status=message.status,
        source=message.source or "api"
    )
    
    if message.sender_type == "OPERATOR":
        try:
            channel = await crud.get_channel(chat['channel_id'])
            
            if channel and channel['type'] == 'AVITO' and chat.get('external_chat_id'):
                from api.chats.avito.avito_factory import create_avito_client, save_token_callback
                
                client = await create_avito_client(
                    channel_id=channel['id'],
                    cashbox_id=user.cashbox_id,
                    on_token_refresh=lambda token_data: save_token_callback(
                        channel['id'],
                        user.cashbox_id,
                        token_data
                    )
                )
                
                if client:
                    try:
                        image_id = None
                        
                        if message.image_url and message.message_type == "IMAGE":
                            try:
                                import aiohttp
                                
                                image_url = message.image_url
                                
                                if 'google.com/imgres' in image_url or 'imgurl=' in image_url:
                                    from urllib.parse import unquote, parse_qs, urlparse
                                    try:
                                        parsed = urlparse(image_url)
                                        params = parse_qs(parsed.query)
                                        if 'imgurl' in params:
                                            real_url = unquote(params['imgurl'][0])
                                            image_url = real_url
                                    except Exception:
                                        pass
                                
                                headers = {}
                                if 'avito' in image_url.lower():
                                    headers['Authorization'] = f"Bearer {client.access_token}"
                                
                                connector = aiohttp.TCPConnector(ssl=False)
                                async with aiohttp.ClientSession(connector=connector) as session:
                                    async with session.get(image_url, headers=headers) as img_response:
                                        if img_response.status == 200:
                                            content_type = img_response.headers.get('Content-Type', '')
                                            
                                            if not content_type.startswith('image/'):
                                                image_id = None
                                            else:
                                                image_data = await img_response.read()

                                                if len(image_data) == 0:
                                                    image_id = None
                                                elif len(image_data) > 24 * 1024 * 1024:  # 24 МБ
                                                    image_id = None
                                                else:
                                                    filename = image_url.split('/')[-1].split('?')[0] or "image.jpg"
                                                    if '.' not in filename:
                                                        if 'png' in content_type:
                                                            filename = "image.png"
                                                        elif 'gif' in content_type:
                                                            filename = "image.gif"
                                                        elif 'webp' in content_type:
                                                            filename = "image.webp"
                                                        else:
                                                            filename = "image.jpg"
                                                    
                                                    upload_result = await client.upload_image(image_data, filename)
                                                    
                                                    if upload_result:
                                                        if isinstance(upload_result, tuple):
                                                            image_id, image_url = upload_result
                                                        else:
                                                            image_id = upload_result
                                                            image_url = None
                                                    else:
                                                        image_id = None
                                                        image_url = None
                                        else:
                                            image_id = None
                            except Exception as e:
                                logger.error(f"Failed to upload image to Avito: {e}", exc_info=True)
                                image_id = None
                                image_url = None
                        
                        if message.message_type == "IMAGE":
                            if not image_id:
                                logger.warning(f"Cannot send IMAGE message: image upload failed. image_url: {message.image_url}")
                                raise Exception("Cannot send IMAGE message: image upload failed")
                            avito_message = await client.send_message(
                                chat_id=chat['external_chat_id'],
                                text=None,
                                image_id=image_id
                            )
                        elif message.content:
                            avito_message = await client.send_message(
                                chat_id=chat['external_chat_id'],
                                text=message.content,
                                image_id=None
                            )
                        else:
                            logger.warning("Cannot send message: no image_id and no text content")
                            raise Exception("Cannot send message: image upload failed and no text provided")
                        
                        external_message_id = avito_message.get('id')
                        if external_message_id:
                            await crud.update_message(
                                db_message['id'],
                                external_message_id=external_message_id,
                                status="DELIVERED"
                            )
                            
                            if image_id and image_url and message.message_type == "IMAGE":
                                try:
                                    from database.db import pictures
                                    await database.execute(
                                        pictures.insert().values(
                                            entity="messages",
                                            entity_id=db_message['id'],
                                            url=image_url,
                                            is_main=False,
                                            is_deleted=False,
                                            owner=user.cashbox_id,
                                            cashbox=user.cashbox_id
                                        )
                                    )
                                except Exception as e:
                                    logger.warning(f"Failed to save image file for message {db_message['id']}: {e}")
                    except Exception as e:
                        logger.error(f"Failed to send message to Avito: {e}", exc_info=True)
                        try:
                            await crud.update_message(db_message['id'], status="FAILED")
                        except Exception:
                            pass
                else:
                    logger.warning(f"Could not create Avito client for channel {channel['id']}, cashbox {user.cashbox_id}")
        except Exception as e:
            logger.error(f"Error sending message to Avito: {e}", exc_info=True)
    
    return db_message


@router.get("/messages/{message_id}", response_model=MessageResponse)
async def get_message(message_id: int, token: str, user = Depends(get_current_user)):
    """Get message by ID (must belong to user's cashbox)"""
    message = await crud.get_message(message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    
    chat = await crud.get_chat(message['chat_id'])
    if chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return message


@router.get("/messages/chat/{chat_id}", response_model=MessagesList)
async def get_chat_messages(chat_id: int, token: str, skip: int = 0, limit: int = 100, user = Depends(get_current_user)):
    """Get messages from chat (must belong to user's cashbox)"""
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    messages = await crud.get_messages(chat_id, skip, limit)
    total = await crud.get_messages_count(chat_id)
    
    messages_list = []
    if messages:
        channel = await crud.get_channel(chat['channel_id'])
        client_avatar = chat.get('contact', {}).get('avatar') if chat.get('contact') else None
        operator_avatar = None
        
        if channel and channel['type'] == 'AVITO':
            try:
                from database.db import channel_credentials
                from api.chats.avito.avito_factory import create_avito_client, save_token_callback
                
                creds = await database.fetch_one(
                    channel_credentials.select().where(
                        (channel_credentials.c.channel_id == chat['channel_id']) &
                        (channel_credentials.c.cashbox_id == user.cashbox_id) &
                        (channel_credentials.c.is_active.is_(True))
                    )
                )
                
                if creds:
                    client = await create_avito_client(
                        channel_id=chat['channel_id'],
                        cashbox_id=user.cashbox_id,
                        on_token_refresh=lambda token_data: save_token_callback(
                            chat['channel_id'],
                            user.cashbox_id,
                            token_data
                        )
                    )
                    
                    if client:
                        chat_info = await client.get_chat_info(chat['external_chat_id'])
                        users = chat_info.get('users', [])
                        avito_user_id = creds.get('avito_user_id')
                        
                        if users:
                            for user_data in users:
                                user_id_in_chat = user_data.get('user_id') or user_data.get('id')
                                if user_id_in_chat:
                                    avatar_url = None
                                    public_profile = user_data.get('public_user_profile', {})
                                    if public_profile:
                                        avatar_data = public_profile.get('avatar', {})
                                        if isinstance(avatar_data, dict):
                                            avatar_url = (
                                                avatar_data.get('default') or
                                                avatar_data.get('images', {}).get('256x256') or
                                                avatar_data.get('images', {}).get('128x128') or
                                                (list(avatar_data.get('images', {}).values())[0] if avatar_data.get('images') else None)
                                            )
                                        elif isinstance(avatar_data, str):
                                            avatar_url = avatar_data
                                    
                                    if avatar_url:
                                        if avito_user_id and user_id_in_chat == avito_user_id:
                                            operator_avatar = avatar_url
                                        elif not client_avatar:
                                            client_avatar = avatar_url
                                            # Сохраняем аватар в БД
                                            if chat.get('chat_contact_id'):
                                                from database.db import chat_contacts
                                                await database.execute(
                                                    chat_contacts.update().where(
                                                        chat_contacts.c.id == chat['chat_contact_id']
                                                    ).values(avatar=avatar_url)
                                                )
            except Exception:
                pass
        
        for msg in messages:
            msg_dict = dict(msg)
            if msg_dict.get('sender_type') == 'CLIENT':
                msg_dict['sender_avatar'] = client_avatar
            elif msg_dict.get('sender_type') == 'OPERATOR':
                msg_dict['sender_avatar'] = operator_avatar
            else:
                msg_dict['sender_avatar'] = None
            messages_list.append(MessageResponse(**msg_dict))
    
    return MessagesList(
        data=messages_list,
        total=total,
        skip=skip,
        limit=limit,
        date=chat.get('last_message_time')
    )



@router.delete("/messages/{message_id}")
async def delete_message(message_id: int, token: str, user = Depends(get_current_user)):
    
    import logging
    logger = logging.getLogger(__name__)
    
    existing_message = await crud.get_message(message_id)
    if not existing_message:
        raise HTTPException(status_code=404, detail="Message not found")
    
    chat = await crud.get_chat(existing_message['chat_id'])
    if chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    if existing_message.get('external_message_id') and chat.get('external_chat_id'):
        try:
            channel = await crud.get_channel(chat['channel_id'])
            
            if channel and channel['type'] == 'AVITO':
                from api.chats.avito.avito_factory import create_avito_client, save_token_callback
                
                client = await create_avito_client(
                    channel_id=channel['id'],
                    cashbox_id=user.cashbox_id,
                    on_token_refresh=lambda token_data: save_token_callback(
                        channel['id'],
                        user.cashbox_id,
                        token_data
                    )
                )
                
                if client:
                    try:
                        deleted = await client.delete_message(
                            chat_id=chat['external_chat_id'],
                            message_id=existing_message['external_message_id']
                        )
                        if deleted:
                            logger.info(f"Message {message_id} deleted in Avito API")
                        else:
                            logger.warning(f"Failed to delete message {message_id} in Avito API")
                    except Exception as e:
                        logger.warning(f"Error deleting message in Avito API: {e}")
        except Exception as e:
            logger.warning(f"Error during Avito message deletion: {e}")
    
    return await crud.delete_message(message_id)


@router.get("/chats/{chat_id}/files/", response_model=list)
async def get_chat_files(chat_id: int, token: str, user = Depends(get_current_user)):
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    if chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    messages = await crud.get_messages(chat_id, skip=0, limit=1000)
    message_ids = [msg['id'] for msg in messages]
    
    if not message_ids:
        return []
    
    query = (
        select(pictures)
        .where(
            pictures.c.entity == "messages",
            pictures.c.entity_id.in_(message_ids),
            pictures.c.is_deleted.is_not(True)
        )
        .order_by(pictures.c.created_at.desc())
    )
    
    files = await database.fetch_all(query)
    return files


@router.get("/messages/{message_id}/files/", response_model=list)
async def get_message_files(message_id: int, token: str, user = Depends(get_current_user)):
    message = await crud.get_message(message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    
    chat = await crud.get_chat(message['chat_id'])
    if chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    query = (
        select(pictures)
        .where(
            pictures.c.entity == "messages",
            pictures.c.entity_id == message_id,
            pictures.c.is_deleted.is_not(True)
        )
        .order_by(pictures.c.created_at.desc())
    )
    
    files = await database.fetch_all(query)
    return files


@router.put("/chats/{chat_id}/chain_client/", response_model=dict)
async def chain_client_endpoint(
    chat_id: int,
    token: str,
    request: ChainClientRequest,
    message_id: Optional[int] = None,
    user = Depends(get_current_user)
):
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return await crud.chain_client(
        chat_id=chat_id,
        message_id=message_id,
        phone=request.phone,
        name=request.name
    )


@router.get("/chats/{chat_id}/managers/", response_model=ManagersInChatResponse)
async def get_managers_in_chat(
    chat_id: int,
    token: str,
    user = Depends(get_current_user)
):
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    if chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    connected_users = chat_manager.get_connected_users(chat_id)
    
    managers = [
        ManagerInChat(
            user_id=user_info['user_id'],
            user_type=user_info['user_type'],
            connected_at=user_info['connected_at']
        )
        for user_info in connected_users
        if user_info['user_type'] == "OPERATOR"
    ]
    
    return ManagersInChatResponse(
        chat_id=chat_id,
        managers=managers,
        total=len(managers)
    )

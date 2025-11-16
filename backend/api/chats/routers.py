from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
from sqlalchemy import select

from api.chats import crud
from api.chats.auth import get_current_user, get_current_user_owner
from api.chats.schemas import (
    ChannelCreate, ChannelUpdate, ChannelResponse,
    ChatCreate, ChatUpdate, ChatResponse,
    MessageCreate, MessageResponse,
    ChainClientRequest
)
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
    """Get channel by ID"""
    channel = await crud.get_channel(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    return channel


@router.get("/channels/", response_model=list)
async def get_channels(token: str, skip: int = 0, limit: int = 100, user = Depends(get_current_user)):
    """Get all channels"""
    return await crud.get_channels(skip, limit)


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
        contragent_id=chat.contragent_id,
        cashbox_id=user.cashbox_id,
        external_chat_id=chat.external_chat_id,
        assigned_operator_id=chat.assigned_operator_id,
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
        status=message.status
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
                                
                                async with aiohttp.ClientSession() as session:
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
                        
                        if image_id or (message.message_type != "IMAGE" and message.content):
                            avito_message = await client.send_message(
                                chat_id=chat['external_chat_id'],
                                text=message.content if message.message_type != "IMAGE" else None,
                                image_id=image_id
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


@router.get("/messages/chat/{chat_id}", response_model=list)
async def get_chat_messages(chat_id: int, token: str, skip: int = 0, limit: int = 100, user = Depends(get_current_user)):
    """Get messages from chat (must belong to user's cashbox)"""
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat['cashbox_id'] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return await crud.get_messages(chat_id, skip, limit)



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

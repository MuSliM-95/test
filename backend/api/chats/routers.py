from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
from sqlalchemy import select

from api.chats import crud
from api.chats.auth import get_current_user, get_current_user_owner
from api.chats.schemas import (
    ChannelCreate, ChannelUpdate, ChannelResponse,
    ChatCreate, ChatUpdate, ChatResponse,
    MessageCreate, MessageUpdate, MessageResponse,
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
    
    if chat.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return chat


@router.get("/chats/", response_model=list)
async def get_chats(
    token: str,
    channel_id: Optional[int] = None,
    contragent_id: Optional[int] = None,
    status: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    user = Depends(get_current_user)
):
    """Get chats with filters (filtered by user's cashbox)"""
    return await crud.get_chats(
        cashbox_id=user.cashbox_id,
        channel_id=channel_id,
        contragent_id=contragent_id,
        status=status,
        skip=skip,
        limit=limit
    )


@router.put("/chats/{chat_id}", response_model=ChatResponse)
async def update_chat(chat_id: int, token: str, chat: ChatUpdate, user = Depends(get_current_user)):
    """Update chat (must belong to user's cashbox)"""
    existing_chat = await crud.get_chat(chat_id)
    if not existing_chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    if existing_chat.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return await crud.update_chat(chat_id, **chat.dict(exclude_unset=True))


@router.delete("/chats/{chat_id}")
async def delete_chat(chat_id: int, token: str, user = Depends(get_current_user)):
    """Delete chat (soft-delete, must belong to user's cashbox)"""
    existing_chat = await crud.get_chat(chat_id)
    if not existing_chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    if existing_chat.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return await crud.delete_chat(chat_id)


@router.post("/messages/", response_model=MessageResponse)
async def create_message(token: str, message: MessageCreate, user = Depends(get_current_user)):
    """Create a new message"""
    chat = await crud.get_chat(message.chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return await crud.create_message_and_update_chat(
        chat_id=message.chat_id,
        sender_type=message.sender_type,
        content=message.content,
        message_type=message.message_type,
        external_message_id=message.external_message_id,
        status=message.status
    )


@router.get("/messages/{message_id}", response_model=MessageResponse)
async def get_message(message_id: int, token: str, user = Depends(get_current_user)):
    """Get message by ID (must belong to user's cashbox)"""
    message = await crud.get_message(message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    
    chat = await crud.get_chat(message.chat_id)
    if chat.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return message


@router.get("/messages/chat/{chat_id}", response_model=list)
async def get_chat_messages(chat_id: int, token: str, skip: int = 0, limit: int = 100, user = Depends(get_current_user)):
    """Get messages from chat (must belong to user's cashbox)"""
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return await crud.get_messages(chat_id, skip, limit)


@router.put("/messages/{message_id}", response_model=MessageResponse)
async def update_message(message_id: int, token: str, message: MessageUpdate, user = Depends(get_current_user)):
    """Update message (must belong to user's cashbox)"""
    existing_message = await crud.get_message(message_id)
    if not existing_message:
        raise HTTPException(status_code=404, detail="Message not found")
    
    chat = await crud.get_chat(existing_message.chat_id)
    if chat.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return await crud.update_message(message_id, **message.dict(exclude_unset=True))


@router.delete("/messages/{message_id}")
async def delete_message(message_id: int, token: str, user = Depends(get_current_user)):
    """Delete message (must belong to user's cashbox)"""
    existing_message = await crud.get_message(message_id)
    if not existing_message:
        raise HTTPException(status_code=404, detail="Message not found")
    
    chat = await crud.get_chat(existing_message.chat_id)
    if chat.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return await crud.delete_message(message_id)


@router.get("/chats/{chat_id}/files/", response_model=list)
async def get_chat_files(chat_id: int, token: str, user = Depends(get_current_user)):
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    if chat.cashbox_id != user.cashbox_id:
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
    
    chat = await crud.get_chat(message.chat_id)
    if chat.cashbox_id != user.cashbox_id:
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
    if chat.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return await crud.chain_client(
        chat_id=chat_id,
        message_id=message_id,
        phone=request.phone,
        name=request.name
    )

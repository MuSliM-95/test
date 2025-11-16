from sqlalchemy import desc, and_, or_, func, select
from database.db import database, channels, chats, chat_messages, contragents
from fastapi import HTTPException
from typing import Optional, List, Dict, Any
from datetime import datetime



async def create_channel(name: str, type: str, description: Optional[str] = None, svg_icon: Optional[str] = None, tags: Optional[dict] = None, api_config_name: Optional[str] = None):
    """Create a new channel"""
    query = channels.insert().values(
        name=name,
        type=type,
        description=description,
        svg_icon=svg_icon,
        tags=tags,
        api_config_name=api_config_name,
        is_active=True
    )
    channel_id = await database.execute(query)
    return await get_channel(channel_id)


async def get_channel(channel_id: int):
    """Get channel by ID"""
    query = channels.select().where(channels.c.id == channel_id)
    return await database.fetch_one(query)


async def get_channel_by_type(channel_type: str):
    """Get channel by type (optimized single lookup)"""
    query = channels.select().where(channels.c.type == channel_type)
    return await database.fetch_one(query)


async def get_channels(skip: int = 0, limit: int = 100):
    """Get all channels with pagination"""
    query = channels.select().offset(skip).limit(limit)
    return await database.fetch_all(query)


async def update_channel(channel_id: int, **kwargs):
    """Update channel"""
    channel = await get_channel(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    
    query = channels.update().where(channels.c.id == channel_id).values(**kwargs)
    await database.execute(query)
    return await get_channel(channel_id)


async def delete_channel(channel_id: int):
    """Delete channel (soft delete - deactivate only, no data loss)"""
    channel = await get_channel(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    
    query = channels.update().where(channels.c.id == channel_id).values(is_active=False)
    await database.execute(query)
    return {"success": True, "message": "Channel deactivated (data preserved)"}



async def create_chat(channel_id: int, cashbox_id: int, contragent_id: Optional[int] = None, external_chat_id: Optional[str] = None, assigned_operator_id: Optional[int] = None, phone: Optional[str] = None, name: Optional[str] = None):
    """Create a new chat (cashbox_id comes from user token)"""
    query = chats.insert().values(
        channel_id=channel_id,
        contragent_id=contragent_id,
        cashbox_id=cashbox_id,
        external_chat_id=external_chat_id,
        assigned_operator_id=assigned_operator_id,
        phone=phone,
        name=name,
        status="ACTIVE"
    )
    chat_id = await database.execute(query)
    return await get_chat(chat_id)


async def get_chat(chat_id: int):
    """Get chat by ID"""
    query = chats.select().where(chats.c.id == chat_id)
    return await database.fetch_one(query)


async def get_chat_by_external_id(channel_id: int, external_chat_id: str, cashbox_id: int):
    """Find chat by external_chat_id, channel_id and cashbox_id (optimized single query)"""
    query = chats.select().where(and_(
        chats.c.channel_id == channel_id,
        chats.c.external_chat_id == external_chat_id,
        chats.c.cashbox_id == cashbox_id
    ))
    return await database.fetch_one(query)


async def get_chats(
    cashbox_id: int, 
    channel_id: Optional[int] = None, 
    contragent_id: Optional[int] = None, 
    status: Optional[str] = None, 
    search: Optional[str] = None,
    skip: int = 0, 
    limit: int = 100
) -> List[Dict[str, Any]]:
    query = select([
        chats.c.id,
        chats.c.channel_id,
        chats.c.contragent_id,
        chats.c.cashbox_id,
        chats.c.external_chat_id,
        chats.c.phone,
        chats.c.name,
        chats.c.status,
        chats.c.assigned_operator_id,
        chats.c.first_message_time,
        chats.c.first_response_time_seconds,
        chats.c.last_message_time,
        chats.c.last_response_time_seconds,
        chats.c.created_at,
        chats.c.updated_at,
        channels.c.name.label('channel_name'),
        channels.c.type.label('channel_type'),
        channels.c.svg_icon.label('channel_icon')
    ]).select_from(
        chats.join(channels, chats.c.channel_id == channels.c.id)
    )
    
    conditions = [chats.c.cashbox_id == cashbox_id] 
    
    if channel_id:
        conditions.append(chats.c.channel_id == channel_id)
    if contragent_id:
        conditions.append(chats.c.contragent_id == contragent_id)
    if status:
        conditions.append(chats.c.status == status)
    if search:
        search_condition = or_(
            chats.c.name.ilike(f"%{search}%"),
            chats.c.phone.ilike(f"%{search}%"),
            chats.c.external_chat_id.ilike(f"%{search}%")
        )
        conditions.append(search_condition)
    
    query = query.where(and_(*conditions))
    query = query.order_by(desc(chats.c.last_message_time), desc(chats.c.created_at))
    query = query.offset(skip).limit(limit)
    
    chats_data = await database.fetch_all(query)
    
    result = []
    for chat_row in chats_data:
        chat_dict = dict(chat_row)
        chat_id = chat_dict['id']
        
        last_message_query = select([chat_messages.c.content]).where(
            chat_messages.c.chat_id == chat_id
        ).order_by(desc(chat_messages.c.created_at)).limit(1)
        last_message = await database.fetch_one(last_message_query)
        
        if last_message and last_message['content']:
            preview = last_message['content'][:100]
            chat_dict['last_message_preview'] = preview
        else:
            chat_dict['last_message_preview'] = None
        
        unread_query = select([func.count(chat_messages.c.id)]).where(
            and_(
                chat_messages.c.chat_id == chat_id,
                chat_messages.c.sender_type == "CLIENT",
                chat_messages.c.status != "READ"
            )
        )
        unread_count = await database.fetch_val(unread_query) or 0
        chat_dict['unread_count'] = unread_count
        
        result.append(chat_dict)
    
    return result


async def update_chat(chat_id: int, **kwargs):
    """Update chat"""
    from datetime import datetime
    
    chat = await get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    if 'first_message_time' in kwargs and kwargs['first_message_time'] is not None:
        dt = kwargs['first_message_time']
        if isinstance(dt, datetime) and dt.tzinfo is not None:
            kwargs['first_message_time'] = dt.replace(tzinfo=None)
    
    if 'last_message_time' in kwargs and kwargs['last_message_time'] is not None:
        dt = kwargs['last_message_time']
        if isinstance(dt, datetime) and dt.tzinfo is not None:
            kwargs['last_message_time'] = dt.replace(tzinfo=None)
    
    query = chats.update().where(chats.c.id == chat_id).values(**kwargs)
    await database.execute(query)
    return await get_chat(chat_id)


async def delete_chat(chat_id: int):
    """Delete chat"""
    chat = await get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    query = chats.delete().where(chats.c.id == chat_id)
    await database.execute(query)
    return {"success": True}



async def create_message(chat_id: int, sender_type: str, content: str, message_type: str = "TEXT", external_message_id: Optional[str] = None, status: str = "SENT"):
    """Create a new message (sender_id removed - use sender_type only)"""
    query = chat_messages.insert().values(
        chat_id=chat_id,
        sender_type=sender_type,
        content=content,
        message_type=message_type,
        external_message_id=external_message_id,
        status=status
    )
    message_id = await database.execute(query)
    return await get_message(message_id)


async def create_message_and_update_chat(
    chat_id: int,
    sender_type: str,
    content: str,
    message_type: str = "TEXT",
    external_message_id: Optional[str] = None,
    status: str = "SENT",
    created_at: Optional[datetime] = None
):
    chat = await get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    message_values = {
        "chat_id": chat_id,
        "sender_type": sender_type,
        "content": content,
        "message_type": message_type,
        "external_message_id": external_message_id,
        "status": status
    }
    
    if created_at:
        message_values["created_at"] = created_at
    
    message_id = await database.execute(
        chat_messages.insert().values(**message_values)
    )
    message = await get_message(message_id)
    current_time = message.created_at if message.created_at else datetime.now()
    
    chat_updates = {}
    
    if sender_type == "CLIENT":
        if chat.first_message_time is None:
            chat_updates['first_message_time'] = current_time
        
        chat_updates['last_message_time'] = current_time
    
    elif sender_type == "OPERATOR":
        if chat.first_response_time_seconds is None and chat.first_message_time is not None:
            time_diff = current_time - chat.first_message_time
            chat_updates['first_response_time_seconds'] = int(time_diff.total_seconds())
        
        last_client_msg = await database.fetch_one(
            chat_messages.select()
            .where(and_(
                chat_messages.c.chat_id == chat_id,
                chat_messages.c.sender_type == "CLIENT"
            ))
            .order_by(desc(chat_messages.c.created_at))
        )
        
        if last_client_msg:
            last_client_time = last_client_msg.created_at
            time_diff = current_time - last_client_time
            chat_updates['last_response_time_seconds'] = int(time_diff.total_seconds())
        
        chat_updates['last_message_time'] = current_time
    
    if chat_updates:
        await update_chat(chat_id, **chat_updates)
    
    return message


async def get_message(message_id: int):
    """Get message by ID"""
    query = chat_messages.select().where(chat_messages.c.id == message_id)
    return await database.fetch_one(query)


async def get_messages(chat_id: int, skip: int = 0, limit: int = 100):
    """Get messages from chat with pagination"""
    query = chat_messages.select().where(chat_messages.c.chat_id == chat_id).offset(skip).limit(limit).order_by(chat_messages.c.created_at)
    return await database.fetch_all(query)


async def update_message(message_id: int, **kwargs):
    """Update message"""
    message = await get_message(message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    
    query = chat_messages.update().where(chat_messages.c.id == message_id).values(**kwargs)
    await database.execute(query)
    return await get_message(message_id)


async def delete_message(message_id: int):
    """Delete message"""
    message = await get_message(message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    
    query = chat_messages.delete().where(chat_messages.c.id == message_id)
    await database.execute(query)
    return {"success": True}


async def chain_client(chat_id: int, message_id: Optional[int] = None, phone: Optional[str] = None, name: Optional[str] = None):
    chat = await get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    if message_id:
        message = await get_message(message_id)
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")
    
    if not phone:
        phone = chat.get('phone')
        if not phone:
            raise HTTPException(status_code=400, detail="Phone number required")
    
    if not name:
        name = chat.get('name')
    
    cashbox_id = chat['cashbox_id']
    channel_id = chat['channel_id']
    
    channel = await get_channel(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    
    channel_type = channel.get('type', 'UNKNOWN') 
    query = contragents.select().where(
        and_(
            contragents.c.phone == phone,
            contragents.c.cashbox == cashbox_id,
            contragents.c.is_deleted.is_not(True)
        )
    )
    existing_contragent = await database.fetch_one(query)
    
    contragent_id = None
    contragent_name = None
    is_new_contragent = False
    
    if existing_contragent:
        contragent_id = existing_contragent['id']
        contragent_name = existing_contragent['name']
        message_result = "Chat linked with existing contragent"
        
        existing_data = existing_contragent.get('data') or {}
        if not isinstance(existing_data, dict):
            existing_data = {}
        
        if 'chat_ids' not in existing_data:
            existing_data['chat_ids'] = []
        
        if chat_id not in existing_data['chat_ids']:
            existing_data['chat_ids'].append(chat_id)
        
        if 'primary_channel' not in existing_data:
            existing_data['primary_channel'] = channel_type
        
        update_contragent_query = contragents.update().where(
            contragents.c.id == contragent_id
        ).values(data=existing_data)
        await database.execute(update_contragent_query)
    else:
        is_new_contragent = True
        contragent_name = name or "Unknown"
        
        contragent_data = {
            "chat_ids": [chat_id],
            "primary_channel": channel_type
        }
        
        insert_query = contragents.insert().values(
            name=contragent_name,
            phone=phone,
            cashbox=cashbox_id,
            is_deleted=False,
            description=f"Канал: {channel_type}",
            external_id=None,
            contragent_type="Покупатель",
            data=contragent_data
        )
        contragent_id = await database.execute(insert_query)
        message_result = "New contragent created and linked to chat"
    
    update_data = {
        "phone": phone,
        "contragent_id": contragent_id
    }
    if name:
        update_data["name"] = name
    
    query = chats.update().where(chats.c.id == chat_id).values(**update_data)
    await database.execute(query)
    
    updated_chat = await get_chat(chat_id)
    
    return {
        "chat": updated_chat,
        "contragent_id": contragent_id,
        "contragent_name": contragent_name,
        "is_new_contragent": is_new_contragent,
        "message": message_result,
        "phone": phone,
        "channel_type": channel_type
    }


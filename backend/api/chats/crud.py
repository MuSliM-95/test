from sqlalchemy import desc, and_, or_, func, select, cast, String
from database.db import database, channels, chats, chat_messages, contragents, channel_credentials, chat_contacts
from fastapi import HTTPException
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
import asyncio



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


async def get_channel_by_id_and_cashbox(channel_id: int, cashbox_id: int):
    query = select([
        channels.c.id,
        channels.c.name,
        channels.c.type,
        channels.c.description,
        channels.c.svg_icon,
        channels.c.tags,
        channels.c.api_config_name,
        channels.c.is_active,
        channels.c.created_at,
        channels.c.updated_at
    ]).select_from(
        channels.join(
            channel_credentials,
            channels.c.id == channel_credentials.c.channel_id
        )
    ).where(
        and_(
            channels.c.id == channel_id,
            channel_credentials.c.cashbox_id == cashbox_id,
            channel_credentials.c.is_active.is_(True),
            channels.c.is_active.is_(True)
        )
    ).limit(1)
    
    return await database.fetch_one(query)


async def get_channel_by_cashbox(cashbox_id: int, channel_type: str = "AVITO"):
    """Get channel for specific cashbox by type through channel_credentials"""
    query = select([
        channels.c.id,
        channels.c.name,
        channels.c.type,
        channels.c.description,
        channels.c.svg_icon,
        channels.c.tags,
        channels.c.api_config_name,
        channels.c.is_active,
        channels.c.created_at,
        channels.c.updated_at
    ]).select_from(
        channels.join(
            channel_credentials,
            channels.c.id == channel_credentials.c.channel_id
        )
    ).where(
        and_(
            channel_credentials.c.cashbox_id == cashbox_id,
            channels.c.type == channel_type,
            channel_credentials.c.is_active.is_(True),
            channels.c.is_active.is_(True)
        )
    ).limit(1)
    
    return await database.fetch_one(query)


async def get_all_channels_by_cashbox(cashbox_id: int, channel_type: str = "AVITO"):
    """Get all channels for specific cashbox by type through channel_credentials"""
    query = select([
        channels.c.id, channels.c.name, channels.c.type, channels.c.description,
        channels.c.svg_icon, channels.c.tags, channels.c.api_config_name,
        channels.c.is_active, channels.c.created_at, channels.c.updated_at
    ]).select_from(
        channels.join(
            channel_credentials,
            channels.c.id == channel_credentials.c.channel_id
        )
    ).where(
        and_(
            channel_credentials.c.cashbox_id == cashbox_id,
            channels.c.type == channel_type,
            channel_credentials.c.is_active.is_(True),
            channels.c.is_active.is_(True)
        )
    )
    return await database.fetch_all(query)


async def get_channel_by_cashbox_and_avito_user(cashbox_id: int, avito_user_id: int, channel_type: str = "AVITO"):
    """Get channel for specific cashbox and avito_user_id through channel_credentials"""
    query = select([
        channels.c.id,
        channels.c.name,
        channels.c.type,
        channels.c.description,
        channels.c.svg_icon,
        channels.c.tags,
        channels.c.api_config_name,
        channels.c.is_active,
        channels.c.created_at,
        channels.c.updated_at
    ]).select_from(
        channels.join(
            channel_credentials,
            channels.c.id == channel_credentials.c.channel_id
        )
    ).where(
        and_(
            channel_credentials.c.cashbox_id == cashbox_id,
            channel_credentials.c.avito_user_id == avito_user_id,
            channels.c.type == channel_type,
            channel_credentials.c.is_active.is_(True)
        )
    ).limit(1)
    
    return await database.fetch_one(query)


async def get_channel_by_cashbox_and_api_key(cashbox_id: int, encrypted_api_key: str, channel_type: str = "AVITO"):
    """Get channel for specific cashbox and api_key through channel_credentials
    Используется для различения разных приложений/бизнесов одного пользователя Avito"""
    query = select([
        channels.c.id,
        channels.c.name,
        channels.c.type,
        channels.c.description,
        channels.c.svg_icon,
        channels.c.tags,
        channels.c.api_config_name,
        channels.c.is_active,
        channels.c.created_at,
        channels.c.updated_at
    ]).select_from(
        channels.join(
            channel_credentials,
            channels.c.id == channel_credentials.c.channel_id
        )
    ).where(
        and_(
            channel_credentials.c.cashbox_id == cashbox_id,
            channel_credentials.c.api_key == encrypted_api_key,
            channels.c.type == channel_type,
            channel_credentials.c.is_active.is_(True)
        )
    ).limit(1)
    
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



async def get_or_create_chat_contact(
    channel_id: int,
    external_contact_id: Optional[str] = None,
    name: Optional[str] = None,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    avatar: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> int:
    """
    Получить или создать chat_contact.
    Возвращает ID chat_contact.
    """
    existing = None
    
    if external_contact_id:
        existing = await database.fetch_one(
            chat_contacts.select().where(
                (chat_contacts.c.channel_id == channel_id) &
                (chat_contacts.c.external_contact_id == external_contact_id)
            )
        )
        
    if not existing and phone:
        existing = await database.fetch_one(
            chat_contacts.select().where(
                (chat_contacts.c.channel_id == channel_id) &
                (chat_contacts.c.phone == phone) &
                (chat_contacts.c.phone.is_not(None))
            )
        )
    
    if not existing and name and not phone:
        existing = await database.fetch_one(
            chat_contacts.select().where(
                (chat_contacts.c.channel_id == channel_id) &
                (chat_contacts.c.name == name) &
                (chat_contacts.c.name.is_not(None)) &
                (chat_contacts.c.phone.is_(None))  # Только если phone тоже null
            )
        )
    
    if existing:
        update_data = {"updated_at": datetime.utcnow()}
        
        if external_contact_id and not existing.get('external_contact_id'):
            update_data['external_contact_id'] = external_contact_id
        
        if name and name != existing.get('name'):
            update_data['name'] = name
        if phone and phone != existing.get('phone'):
            update_data['phone'] = phone
        if email and email != existing.get('email'):
            update_data['email'] = email
        if avatar and avatar != existing.get('avatar'):
            update_data['avatar'] = avatar
        
        if len(update_data) > 1:
            await database.execute(
                chat_contacts.update().where(
                    chat_contacts.c.id == existing['id']
                ).values(**update_data)
            )
        
        return existing['id']
    
    # Создаем новый контакт
    contact_id = await database.execute(
        chat_contacts.insert().values(
            channel_id=channel_id,
            external_contact_id=external_contact_id,
            name=name,
            phone=phone,
            email=email,
            avatar=avatar,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
    )
    return contact_id


async def get_chat_contact(contact_id: int) -> Optional[Dict[str, Any]]:
    """Получить chat_contact по ID"""
    contact = await database.fetch_one(
        chat_contacts.select().where(chat_contacts.c.id == contact_id)
    )
    return dict(contact) if contact else None


async def create_chat(
    channel_id: int,
    cashbox_id: int,
    external_chat_id: Optional[str] = None,
    assigned_operator_id: Optional[int] = None,
    chat_contact_id: Optional[int] = None,
    external_chat_id_for_contact: Optional[str] = None,
    name: Optional[str] = None,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    avatar: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
):
    """
    Create a new chat.
    Если chat_contact_id не указан, создается или находится chat_contact на основе переданных данных.
    """
    if not chat_contact_id:
        chat_contact_id = await get_or_create_chat_contact(
            channel_id=channel_id,
            external_contact_id=external_chat_id_for_contact,  
            name=name,
            phone=phone,
            email=email,
            avatar=avatar
        )
    
    query = chats.insert().values(
        channel_id=channel_id,
        chat_contact_id=chat_contact_id,
        cashbox_id=cashbox_id,
        external_chat_id=external_chat_id or "",
        assigned_operator_id=assigned_operator_id,
        status="ACTIVE",
        metadata=metadata
    )
    chat_id = await database.execute(query)
    return await get_chat(chat_id)


async def get_chat(chat_id: int):
    """Get chat by ID with additional fields"""
    query = select([
        chats.c.id,
        chats.c.channel_id,
        chats.c.chat_contact_id,
        chats.c.cashbox_id,
        chats.c.external_chat_id,
        chats.c.status,
        chats.c.assigned_operator_id,
        chats.c.first_message_time,
        chats.c.first_response_time_seconds,
        chats.c.last_message_time,
        chats.c.last_response_time_seconds,
        chats.c.metadata,
        chats.c.created_at,
        chats.c.updated_at,
        channels.c.name.label('channel_name'),
        channels.c.type.label('channel_type'),
        channels.c.svg_icon.label('channel_icon'),
        chat_contacts.c.name.label('contact_name'),
        chat_contacts.c.phone.label('contact_phone'),
        chat_contacts.c.email.label('contact_email'),
        chat_contacts.c.avatar.label('contact_avatar'),
        chat_contacts.c.contragent_id.label('contact_contragent_id')
    ]).select_from(
        chats.join(channels, chats.c.channel_id == channels.c.id)
        .outerjoin(chat_contacts, chats.c.chat_contact_id == chat_contacts.c.id)
    ).where(chats.c.id == chat_id)
    
    chat_row = await database.fetch_one(query)
    if not chat_row:
        return None
    
    chat_dict = dict(chat_row)
    
    contact_info = None
    if any([
        chat_dict.get('contact_name'),
        chat_dict.get('contact_phone'),
        chat_dict.get('contact_email'),
        chat_dict.get('contact_avatar'),
        chat_dict.get('contact_contragent_id')
    ]):
        contact_info = {
            'name': chat_dict.get('contact_name'),
            'phone': chat_dict.get('contact_phone'),
            'email': chat_dict.get('contact_email'),
            'avatar': chat_dict.get('contact_avatar'),
            'contragent_id': chat_dict.get('contact_contragent_id')
        }
    
    chat_dict.pop('contact_name', None)
    chat_dict.pop('contact_phone', None)
    chat_dict.pop('contact_email', None)
    chat_dict.pop('contact_avatar', None)
    chat_dict.pop('contact_contragent_id', None)
    
    chat_dict['contact'] = contact_info
    
    name = None
    metadata = chat_dict.get('metadata')
    if metadata is not None:
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except:
                metadata = None
        if isinstance(metadata, dict):
            name = metadata.get('ad_title')
            if not name:
                context = metadata.get('context', {})
                if isinstance(context, dict):
                    value = context.get('value', {})
                    if isinstance(value, dict):
                        name = value.get('title')
            if not name:
                value = metadata.get('value', {})
                if isinstance(value, dict):
                    name = value.get('title')
    chat_dict['metadata'] = metadata
    if not chat_dict.get('name'):
        chat_dict['name'] = name
    
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
    
    is_avito_chat = (
        chat_dict.get('channel_type') == 'AVITO' or 
        (chat_dict.get('external_chat_id') and chat_dict.get('external_chat_id', '').startswith('u2'))
    )
    
    if is_avito_chat:
        try:
            from database.db import channel_credentials
            from api.chats.avito.avito_factory import create_avito_client, save_token_callback
            
            if not chat_dict.get('channel_type'):
                channel = await get_channel(chat_dict['channel_id'])
                if channel and channel.get('type') == 'AVITO':
                    chat_dict['channel_type'] = 'AVITO'
                    chat_dict['channel_name'] = channel.get('name')
                    chat_dict['channel_icon'] = channel.get('svg_icon')
            
            if chat_dict.get('channel_type') == 'AVITO':
                creds = await database.fetch_one(
                    channel_credentials.select().where(
                        (channel_credentials.c.channel_id == chat_dict['channel_id']) &
                        (channel_credentials.c.cashbox_id == chat_dict['cashbox_id']) &
                        (channel_credentials.c.is_active.is_(True))
                    )
                )
                
                if creds:
                    client = await create_avito_client(
                        channel_id=chat_dict['channel_id'],
                        cashbox_id=chat_dict['cashbox_id'],
                        on_token_refresh=lambda token_data: save_token_callback(
                            chat_dict['channel_id'],
                            chat_dict['cashbox_id'],
                            token_data
                        )
                    )
                    
                    if client:
                        chat_info = await client.get_chat_info(chat_dict['external_chat_id'])
                        users = chat_info.get('users', [])
                        avito_user_id = creds.get('avito_user_id')
                        
                        if not chat_dict.get('name'):
                            context = chat_info.get('context', {})
                            if isinstance(context, dict):
                                value = context.get('value', {})
                                if isinstance(value, dict):
                                    title = value.get('title')
                                    if title:
                                        chat_dict['name'] = title
                        
                        if users:
                            for user in users:
                                user_id_in_chat = user.get('user_id') or user.get('id')
                                if avito_user_id:
                                    if user_id_in_chat and user_id_in_chat != avito_user_id:
                                        avatar_url = None
                                        public_profile = user.get('public_user_profile', {})
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
                                            if chat_dict.get('contact'):
                                                chat_dict['contact']['avatar'] = avatar_url
                                            else:
                                                chat_dict['contact'] = {'avatar': avatar_url}
                                            
                                            if chat_dict.get('chat_contact_id'):
                                                await database.execute(
                                                    chat_contacts.update().where(
                                                        chat_contacts.c.id == chat_dict['chat_contact_id']
                                                    ).values(avatar=avatar_url)
                                                )
                                            break
                                else:
                                    public_profile = user.get('public_user_profile', {})
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
                                            if chat_dict.get('contact'):
                                                chat_dict['contact']['avatar'] = avatar_url
                                            else:
                                                chat_dict['contact'] = {'avatar': avatar_url}
                                            
                                            if chat_dict.get('chat_contact_id'):
                                                await database.execute(
                                                    chat_contacts.update().where(
                                                        chat_contacts.c.id == chat_dict['chat_contact_id']
                                                    ).values(avatar=avatar_url)
                                                )
                                            break
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to get contact_avatar for chat {chat_id}: {e}")
    
    return chat_dict


async def get_chat_by_external_id(channel_id: int, external_chat_id: str, cashbox_id: int):
    chat = await database.fetch_one(
        chats.select().where(and_(
            chats.c.channel_id == channel_id,
            chats.c.external_chat_id == external_chat_id,
            chats.c.cashbox_id == cashbox_id
        ))
    )
    if chat:
        return await get_chat(chat['id'])
    return None


async def get_chats(
    cashbox_id: int, 
    channel_id: Optional[int] = None, 
    contragent_id: Optional[int] = None, 
    status: Optional[str] = None, 
    search: Optional[str] = None,
    created_from: Optional[datetime] = None,
    created_to: Optional[datetime] = None,
    updated_from: Optional[datetime] = None,
    updated_to: Optional[datetime] = None,
    sort_by: Optional[str] = None,
    sort_order: Optional[str] = "desc",
    skip: int = 0, 
    limit: int = 100,
    with_avito_info: bool = False 
) -> List[Dict[str, Any]]:
    last_msg_correlated = (
        select([
            func.left(
                func.coalesce(cast(chat_messages.c.content, String), ''),
                100
            )
        ])
        .where(chat_messages.c.chat_id == chats.c.id)
        .order_by(desc(chat_messages.c.created_at))
        .limit(1)
        .correlate(chats)
        .scalar_subquery()
    )
    
    unread_count_subquery = (
        select([
            chat_messages.c.chat_id,
            func.count(chat_messages.c.id).label('unread_count')
        ])
        .where(
            and_(
                chat_messages.c.sender_type == "CLIENT",
                chat_messages.c.status != "READ"
            )
        )
        .group_by(chat_messages.c.chat_id)
        .subquery('unread')
    )
    
    query = select([
        chats.c.id,
        chats.c.channel_id,
        chats.c.chat_contact_id,
        chats.c.cashbox_id,
        chats.c.external_chat_id,
        chats.c.status,
        chats.c.assigned_operator_id,
        chats.c.first_message_time,
        chats.c.first_response_time_seconds,
        chats.c.last_message_time,
        chats.c.last_response_time_seconds,
        chats.c.metadata,
        chats.c.created_at,
        chats.c.updated_at,
        channels.c.name.label('channel_name'),
        channels.c.type.label('channel_type'),
        channels.c.svg_icon.label('channel_icon'),
        chat_contacts.c.name.label('contact_name'),
        chat_contacts.c.phone.label('contact_phone'),
        chat_contacts.c.email.label('contact_email'),
        chat_contacts.c.avatar.label('contact_avatar'),
        chat_contacts.c.contragent_id.label('contact_contragent_id'),
        last_msg_correlated.label('last_message_preview'),
        func.coalesce(unread_count_subquery.c.unread_count, 0).label('unread_count')
    ]).select_from(
        chats.join(channels, chats.c.channel_id == channels.c.id)
        .outerjoin(chat_contacts, chats.c.chat_contact_id == chat_contacts.c.id)
        .outerjoin(unread_count_subquery, unread_count_subquery.c.chat_id == chats.c.id)
    )
    
    conditions = [chats.c.cashbox_id == cashbox_id] 
    
    if channel_id:
        conditions.append(chats.c.channel_id == channel_id)
    if contragent_id:
        conditions.append(chat_contacts.c.contragent_id == contragent_id)
    if status:
        conditions.append(chats.c.status == status)
    if search:
        search_condition = or_(
            chat_contacts.c.name.ilike(f"%{search}%"),
            chat_contacts.c.phone.ilike(f"%{search}%"),
            chats.c.external_chat_id.ilike(f"%{search}%")
        )
        conditions.append(search_condition)
    
    if created_from:
        conditions.append(chats.c.created_at >= created_from)
    if created_to:
        conditions.append(chats.c.created_at <= created_to)
    
    if updated_from:
        conditions.append(chats.c.updated_at >= updated_from)
    if updated_to:
        conditions.append(chats.c.updated_at <= updated_to)
    
    query = query.where(and_(*conditions))

    if sort_by:
        sort_column = None
        if sort_by == "created_at":
            sort_column = chats.c.created_at
        elif sort_by == "updated_at":
            sort_column = chats.c.updated_at
        elif sort_by == "last_message_time":
            sort_column = chats.c.last_message_time
        elif sort_by == "name":
            sort_column = chat_contacts.c.name
        
        if sort_column:
            if sort_order and sort_order.lower() == "asc":
                query = query.order_by(sort_column.asc().nulls_last())
            else:
                query = query.order_by(sort_column.desc().nulls_last())
        else:
            query = query.order_by(desc(chats.c.updated_at).nulls_last())
    else:
        query = query.order_by(desc(chats.c.updated_at).nulls_last())
    
    query = query.offset(skip).limit(limit)
    
    chats_data = await database.fetch_all(query)
    
    avito_chats_to_fetch = []
    if with_avito_info:
        for chat_row in chats_data:
            chat_dict = dict(chat_row)
            is_avito_chat = (
                chat_dict.get('channel_type') == 'AVITO' or 
                (chat_dict.get('external_chat_id') and chat_dict.get('external_chat_id', '').startswith('u2'))
            )
            if is_avito_chat:
                avito_chats_to_fetch.append(chat_dict)
    
    channel_creds_cache = {}
    if with_avito_info and avito_chats_to_fetch:
        from database.db import channel_credentials
        unique_channel_ids = list(set(chat['channel_id'] for chat in avito_chats_to_fetch))
        if unique_channel_ids:
            creds_query = channel_credentials.select().where(
                and_(
                    channel_credentials.c.channel_id.in_(unique_channel_ids),
                    channel_credentials.c.cashbox_id == cashbox_id,
                    channel_credentials.c.is_active.is_(True)
                )
            )
            all_creds = await database.fetch_all(creds_query)
            channel_creds_cache = {cred['channel_id']: cred for cred in all_creds}
    
    avito_info_cache = {}
    if with_avito_info and avito_chats_to_fetch:
        async def fetch_avito_chat_info(chat_dict):
            try:
                from api.chats.avito.avito_factory import create_avito_client, save_token_callback
                
                channel_id = chat_dict['channel_id']
                creds = channel_creds_cache.get(channel_id)
                
                if not creds:
                    return None
                
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
                    return None
                
                chat_info = await client.get_chat_info(chat_dict['external_chat_id'])
                return (chat_dict['id'], chat_info, creds)
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to fetch Avito info for chat {chat_dict.get('id')}: {e}")
                return None
        
        avito_tasks = [fetch_avito_chat_info(chat) for chat in avito_chats_to_fetch]
        avito_results = await asyncio.gather(*avito_tasks, return_exceptions=True)
        
        for result in avito_results:
            if result and not isinstance(result, Exception):
                chat_id, chat_info, creds = result
                avito_info_cache[chat_id] = (chat_info, creds)
    
    result = []
    for chat_row in chats_data:
        chat_dict = dict(chat_row)
        chat_id = chat_dict['id']
        
        if chat_dict.get('unread_count') is not None:
            chat_dict['unread_count'] = int(chat_dict['unread_count'])
        else:
            chat_dict['unread_count'] = 0
        
        contact_info = None
        if any([
            chat_dict.get('contact_name'),
            chat_dict.get('contact_phone'),
            chat_dict.get('contact_email'),
            chat_dict.get('contact_avatar'),
            chat_dict.get('contact_contragent_id')
        ]):
            contact_info = {
                'name': chat_dict.get('contact_name'),
                'phone': chat_dict.get('contact_phone'),
                'email': chat_dict.get('contact_email'),
                'avatar': chat_dict.get('contact_avatar'),
                'contragent_id': chat_dict.get('contact_contragent_id')
            }
        
        # Удаляем старые поля контакта
        chat_dict.pop('contact_name', None)
        chat_dict.pop('contact_phone', None)
        chat_dict.pop('contact_email', None)
        chat_dict.pop('contact_avatar', None)
        chat_dict.pop('contact_contragent_id', None)
        
        # Добавляем объект contact
        chat_dict['contact'] = contact_info
        
        name = None
        metadata = chat_dict.get('metadata')
        if metadata is not None:
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except:
                    metadata = None
            if isinstance(metadata, dict):
                name = metadata.get('ad_title')
                if not name:
                    context = metadata.get('context', {})
                    if isinstance(context, dict):
                        value = context.get('value', {})
                        if isinstance(value, dict):
                            name = value.get('title')
                if not name:
                    value = metadata.get('value', {})
                    if isinstance(value, dict):
                        name = value.get('title')
        chat_dict['metadata'] = metadata
        chat_dict['name'] = name
        
        is_avito_chat = (
            chat_dict.get('channel_type') == 'AVITO' or 
            (chat_dict.get('external_chat_id') and chat_dict.get('external_chat_id', '').startswith('u2'))
        )
        
        if is_avito_chat and with_avito_info:
            try:
                avito_data = avito_info_cache.get(chat_id)
                if avito_data:
                    chat_info, creds = avito_data
                    users = chat_info.get('users', [])
                    avito_user_id = creds.get('avito_user_id')
                    
                    if not chat_dict.get('name'):
                        context = chat_info.get('context', {})
                        if isinstance(context, dict):
                            value = context.get('value', {})
                            if isinstance(value, dict):
                                title = value.get('title')
                                if title:
                                    chat_dict['name'] = title
                    
                    if users:
                        for user in users:
                            user_id_in_chat = user.get('user_id') or user.get('id')
                            if avito_user_id:
                                if user_id_in_chat and user_id_in_chat != avito_user_id:
                                    avatar_url = None
                                    public_profile = user.get('public_user_profile', {})
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
                                        if chat_dict.get('contact'):
                                            chat_dict['contact']['avatar'] = avatar_url
                                        else:
                                            chat_dict['contact'] = {'avatar': avatar_url}
                                        
                                        if chat_dict.get('chat_contact_id'):
                                            await database.execute(
                                                chat_contacts.update().where(
                                                    chat_contacts.c.id == chat_dict['chat_contact_id']
                                                ).values(avatar=avatar_url)
                                            )
                                        break
                            else:
                                public_profile = user.get('public_user_profile', {})
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
                                        if chat_dict.get('contact'):
                                            chat_dict['contact']['avatar'] = avatar_url
                                        else:
                                            chat_dict['contact'] = {'avatar': avatar_url}
                                        
                                        if chat_dict.get('chat_contact_id'):
                                            await database.execute(
                                                chat_contacts.update().where(
                                                    chat_contacts.c.id == chat_dict['chat_contact_id']
                                                ).values(avatar=avatar_url)
                                            )
                                        break
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to get contact_avatar for chat {chat_dict.get('id')}: {e}")
        
        result.append(chat_dict)
    
    return result


async def update_chat(chat_id: int, **kwargs):
    """Update chat. Note: phone and name should be updated via chat_contact."""
    from datetime import datetime
    
    chat = await get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    # Удаляем phone и name из kwargs, так как они теперь в chat_contact
    # Если нужно обновить phone/name, используйте update_chat_contact
    chat_contact_update = {}
    if 'phone' in kwargs:
        chat_contact_update['phone'] = kwargs.pop('phone')
    if 'name' in kwargs:
        chat_contact_update['name'] = kwargs.pop('name')
    
    # Обновляем chat_contact, если нужно
    if chat_contact_update:
        chat_contact_id = chat.get('chat_contact_id')
        if chat_contact_id:
            chat_contact_update['updated_at'] = datetime.utcnow()
            await database.execute(
                chat_contacts.update().where(
                    chat_contacts.c.id == chat_contact_id
                ).values(**chat_contact_update)
            )
    
    if 'first_message_time' in kwargs and kwargs['first_message_time'] is not None:
        dt = kwargs['first_message_time']
        if isinstance(dt, datetime) and dt.tzinfo is not None:
            kwargs['first_message_time'] = dt.replace(tzinfo=None)
    
    last_message_time_normalized = None
    if 'last_message_time' in kwargs and kwargs['last_message_time'] is not None:
        dt = kwargs['last_message_time']
        if isinstance(dt, datetime):
            if dt.tzinfo is not None:
                last_message_time_normalized = dt.replace(tzinfo=None)
            else:
                last_message_time_normalized = dt
            kwargs['last_message_time'] = last_message_time_normalized
    
    if last_message_time_normalized is not None:
        kwargs['updated_at'] = last_message_time_normalized
    else:
        kwargs['updated_at'] = datetime.utcnow()
    
    if kwargs:  # Обновляем чат только если есть что обновлять
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



async def create_message(chat_id: int, sender_type: str, content: str, message_type: str = "TEXT", external_message_id: Optional[str] = None, status: str = "SENT", source: Optional[str] = None):
    """Create a new message"""
    query = chat_messages.insert().values(
        chat_id=chat_id,
        sender_type=sender_type,
        content=content,
        message_type=message_type,
        external_message_id=external_message_id,
        status=status,
        source=source
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
    created_at: Optional[datetime] = None,
    source: Optional[str] = None
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
        "status": status,
        "source": source
    }
    
    if created_at:
        message_values["created_at"] = created_at
    
    message_id = await database.execute(
        chat_messages.insert().values(**message_values)
    )
    message = await get_message(message_id)
    if isinstance(message, dict):
        current_time = message.get('created_at') or datetime.now()
    else:
        current_time = message.created_at if message.created_at else datetime.now()
    
    chat_updates = {}
    
    if sender_type == "CLIENT":
        if chat.get('first_message_time') is None:
            chat_updates['first_message_time'] = current_time
        
        chat_updates['last_message_time'] = current_time
    
    elif sender_type == "OPERATOR":
        first_message_time = chat.get('first_message_time')
        if chat.get('first_response_time_seconds') is None and first_message_time is not None:
            time_diff = current_time - first_message_time
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
            last_client_time = last_client_msg.get('created_at') if isinstance(last_client_msg, dict) else last_client_msg.created_at
            time_diff = current_time - last_client_time
            chat_updates['last_response_time_seconds'] = int(time_diff.total_seconds())
        
        chat_updates['last_message_time'] = current_time
    else:
        chat_updates['last_message_time'] = current_time
    
    if not chat_updates.get('last_message_time'):
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


async def get_messages_count(chat_id: int):
    """Get total count of messages in chat"""
    query = select([func.count(chat_messages.c.id)]).where(chat_messages.c.chat_id == chat_id)
    result = await database.fetch_one(query)
    return result[0] if result else 0


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
    """
    Привязать chat_contact к contragent.
    Обновляет chat_contact.contragent_id.
    """
    chat = await get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    if message_id:
        message = await get_message(message_id)
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")
    
    chat_contact_id = chat.get('chat_contact_id')
    if not chat_contact_id:
        raise HTTPException(status_code=400, detail="Chat has no associated contact")
    
    # Получаем данные из chat_contact
    chat_contact = await get_chat_contact(chat_contact_id)
    if not chat_contact:
        raise HTTPException(status_code=404, detail="Chat contact not found")
    
    # Используем переданные данные или данные из chat_contact
    if not phone:
        phone = chat_contact.get('phone') or (chat.get('contact', {}).get('phone') if chat.get('contact') else None)
        if not phone:
            raise HTTPException(status_code=400, detail="Phone number required")
    
    if not name:
        name = chat_contact.get('name') or (chat.get('contact', {}).get('name') if chat.get('contact') else None)
    
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
        message_result = "Chat contact linked with existing contragent"
        
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
        message_result = "New contragent created and linked to chat contact"
    
    # Обновляем chat_contact: привязываем к contragent и обновляем данные
    update_contact_data = {
        "contragent_id": contragent_id,
        "updated_at": datetime.utcnow()
    }
    
    if phone and phone != chat_contact.get('phone'):
        update_contact_data["phone"] = phone
    
    if name and name != chat_contact.get('name'):
        update_contact_data["name"] = name
    
    await database.execute(
        chat_contacts.update().where(
            chat_contacts.c.id == chat_contact_id
        ).values(**update_contact_data)
    )
    
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


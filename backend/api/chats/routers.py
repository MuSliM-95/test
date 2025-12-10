import logging
from datetime import datetime
from typing import Optional

from api.chats import crud
from api.chats.auth import get_current_user, get_current_user_owner
from api.chats.schemas import (
    ChainClientRequest,
    ChannelCreate,
    ChannelResponse,
    ChannelUpdate,
    ChatCreate,
    ChatResponse,
    ChatUpdate,
    ManagerInChat,
    ManagersInChatResponse,
    MessageCreate,
    MessageResponse,
    MessagesList,
)
from api.chats.websocket import chat_manager
from database.db import database, pictures
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chats", tags=["chats"])


@router.post("/channels/", response_model=ChannelResponse)
async def create_channel(
    token: str, channel: ChannelCreate, user=Depends(get_current_user_owner)
):
    """Create a new channel (owner only)"""
    return await crud.create_channel(
        name=channel.name,
        type=channel.type,
        description=channel.description,
        svg_icon=channel.svg_icon,
        tags=channel.tags,
        api_config_name=channel.api_config_name,
    )


@router.get("/channels/{channel_id}", response_model=ChannelResponse)
async def get_channel(channel_id: int, token: str, user=Depends(get_current_user)):
    channel = await crud.get_channel_by_id_and_cashbox(channel_id, user.cashbox_id)
    if not channel:
        raise HTTPException(
            status_code=404, detail="Channel not found or access denied"
        )
    return channel


@router.get("/channels/", response_model=list)
async def get_channels(
    token: str, skip: int = 0, limit: int = 100, user=Depends(get_current_user)
):
    return await crud.get_all_channels_by_cashbox(user.cashbox_id)


@router.put("/channels/{channel_id}", response_model=ChannelResponse)
async def update_channel(
    channel_id: int,
    token: str,
    channel: ChannelUpdate,
    user=Depends(get_current_user_owner),
):
    """Update channel (owner only)"""
    return await crud.update_channel(channel_id, **channel.dict(exclude_unset=True))


@router.delete("/channels/{channel_id}")
async def delete_channel(
    channel_id: int, token: str, user=Depends(get_current_user_owner)
):
    """Delete channel (owner only, soft-delete)"""
    return await crud.delete_channel(channel_id)


@router.post("/chats/", response_model=ChatResponse)
async def create_chat(token: str, chat: ChatCreate, user=Depends(get_current_user)):
    """Create a new chat (cashbox_id from token)"""
    return await crud.create_chat(
        channel_id=chat.channel_id,
        cashbox_id=user.cashbox_id,
        external_chat_id=chat.external_chat_id,
        assigned_operator_id=chat.assigned_operator_id,
        external_chat_id_for_contact=chat.external_chat_id,
        phone=chat.phone,
        name=chat.name,
    )


@router.get("/chats/{chat_id}", response_model=ChatResponse)
async def get_chat(chat_id: int, token: str, user=Depends(get_current_user)):
    """Get chat by ID (must belong to user's cashbox)"""
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    return chat


@router.get("/chats/", response_model=list)
async def get_chats(
    token: str,
    channel_id: Optional[int] = None,
    contragent_id: Optional[int] = None,
    status: Optional[str] = None,
    search: Optional[str] = Query(
        None, description="Поиск по имени контакта или названию объявления"
    ),
    created_from: Optional[datetime] = Query(
        None, description="Фильтр: дата создания от (ISO 8601)"
    ),
    created_to: Optional[datetime] = Query(
        None, description="Фильтр: дата создания до (ISO 8601)"
    ),
    updated_from: Optional[datetime] = Query(
        None, description="Фильтр: дата обновления от (ISO 8601)"
    ),
    updated_to: Optional[datetime] = Query(
        None, description="Фильтр: дата обновления до (ISO 8601)"
    ),
    sort_by: Optional[str] = Query(
        None,
        description="Сортировка по полю: created_at, updated_at, last_message_time, name",
    ),
    sort_order: Optional[str] = Query(
        "desc", description="Порядок сортировки: asc или desc"
    ),
    skip: int = 0,
    limit: int = 100,
    user=Depends(get_current_user),
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
        limit=limit,
    )


@router.put("/chats/{chat_id}", response_model=ChatResponse)
async def update_chat(
    chat_id: int, token: str, chat: ChatUpdate, user=Depends(get_current_user)
):
    """Update chat (must belong to user's cashbox)"""
    existing_chat = await crud.get_chat(chat_id)
    if not existing_chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if existing_chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    return await crud.update_chat(chat_id, **chat.dict(exclude_unset=True))


@router.delete("/chats/{chat_id}")
async def delete_chat(chat_id: int, token: str, user=Depends(get_current_user)):
    import logging

    logger = logging.getLogger(__name__)

    existing_chat = await crud.get_chat(chat_id)
    if not existing_chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if existing_chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    if existing_chat.get("external_chat_id"):
        try:
            channel = await crud.get_channel(existing_chat["channel_id"])

            if channel and channel["type"] == "AVITO":
                from api.chats.avito.avito_factory import (
                    create_avito_client,
                    save_token_callback,
                )

                client = await create_avito_client(
                    channel_id=channel["id"],
                    cashbox_id=user.cashbox_id,
                    on_token_refresh=lambda token_data: save_token_callback(
                        channel["id"], user.cashbox_id, token_data
                    ),
                )

                if client:
                    try:
                        closed = await client.close_chat(
                            existing_chat["external_chat_id"]
                        )
                        if closed:
                            logger.info(f"Chat {chat_id} closed in Avito API")
                        else:
                            logger.warning(
                                f"Failed to close chat {chat_id} in Avito API"
                            )
                    except Exception as e:
                        logger.warning(f"Error closing chat in Avito API: {e}")
        except Exception as e:
            logger.warning(f"Error during Avito chat closure: {e}")

    return await crud.update_chat(chat_id, status="CLOSED")


@router.post("/messages/", response_model=MessageResponse)
async def create_message(
    token: str, message: MessageCreate, user=Depends(get_current_user)
):
    """Create a new message"""
    import logging

    logger = logging.getLogger(__name__)

    chat = await crud.get_chat(message.chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    has_image = message.message_type == "IMAGE" and message.image_url
    has_text = message.content and message.content.strip()

    if has_image and has_text:
        db_message_image = await crud.create_message_and_update_chat(
            chat_id=message.chat_id,
            sender_type=message.sender_type,
            content="",  # Без текста
            message_type="IMAGE",
            external_message_id=None,
            status=message.status,
            source=message.source or "api",
        )

        db_message_text = await crud.create_message_and_update_chat(
            chat_id=message.chat_id,
            sender_type=message.sender_type,
            content=message.content,
            message_type="TEXT",
            external_message_id=None,
            status=message.status,
            source=message.source or "api",
        )

        db_message = db_message_image
    else:
        db_message = await crud.create_message_and_update_chat(
            chat_id=message.chat_id,
            sender_type=message.sender_type,
            content=message.content,
            message_type=message.message_type,
            external_message_id=None,
            status=message.status,
            source=message.source or "api",
        )
        db_message_text = None

    if message.sender_type == "OPERATOR":
        try:
            channel = await crud.get_channel(chat["channel_id"])

            if channel and channel["type"] == "AVITO" and chat.get("external_chat_id"):
                from api.chats.avito.avito_factory import (
                    create_avito_client,
                    save_token_callback,
                )

                client = await create_avito_client(
                    channel_id=channel["id"],
                    cashbox_id=user.cashbox_id,
                    on_token_refresh=lambda token_data: save_token_callback(
                        channel["id"], user.cashbox_id, token_data
                    ),
                )

                if client:
                    try:
                        image_id = None
                        image_url_for_db = None
                        image_data = None
                        filename = "image.jpg"

                        if message.image_url and (
                            message.message_type == "IMAGE" or (has_image and has_text)
                        ):
                            try:
                                import base64
                                import io
                                from os import environ
                                from uuid import uuid4

                                import aioboto3
                                import aiohttp

                                image_url = message.image_url
                                content_type = None

                                if image_url.startswith("data:"):
                                    try:
                                        header, encoded = image_url.split(",", 1)
                                        content_type = header.split(";")[0].split(":")[
                                            1
                                        ]
                                        image_data = base64.b64decode(encoded)

                                        if "png" in content_type:
                                            filename = "image.png"
                                        elif "gif" in content_type:
                                            filename = "image.gif"
                                        elif "webp" in content_type:
                                            filename = "image.webp"
                                        elif (
                                            "jpeg" in content_type
                                            or "jpg" in content_type
                                        ):
                                            filename = "image.jpg"
                                        else:
                                            filename = "image.jpg"
                                    except Exception as e:
                                        logger.error(
                                            f"Failed to parse data URL: {e}",
                                            exc_info=True,
                                        )
                                        image_data = None

                                if image_data is None:
                                    if (
                                        "google.com/imgres" in image_url
                                        or "imgurl=" in image_url
                                    ):
                                        from urllib.parse import (
                                            parse_qs,
                                            unquote,
                                            urlparse,
                                        )

                                        try:
                                            parsed = urlparse(image_url)
                                            params = parse_qs(parsed.query)
                                            if "imgurl" in params:
                                                real_url = unquote(params["imgurl"][0])
                                                image_url = real_url
                                        except Exception:
                                            pass

                                    headers = {}
                                    if "avito" in image_url.lower():
                                        headers["Authorization"] = (
                                            f"Bearer {client.access_token}"
                                        )

                                    connector = aiohttp.TCPConnector(ssl=False)
                                    async with aiohttp.ClientSession(
                                        connector=connector
                                    ) as session:
                                        async with session.get(
                                            image_url, headers=headers
                                        ) as img_response:
                                            if img_response.status == 200:
                                                content_type = img_response.headers.get(
                                                    "Content-Type", ""
                                                )

                                                if not content_type.startswith(
                                                    "image/"
                                                ):
                                                    image_data = None
                                                else:
                                                    image_data = (
                                                        await img_response.read()
                                                    )

                                                    if len(image_data) == 0:
                                                        image_data = None
                                                    elif (
                                                        len(image_data)
                                                        > 24 * 1024 * 1024
                                                    ):
                                                        image_data = None
                                                    else:
                                                        filename = (
                                                            image_url.split("/")[
                                                                -1
                                                            ].split("?")[0]
                                                            or "image.jpg"
                                                        )
                                                        if "." not in filename:
                                                            if "png" in content_type:
                                                                filename = "image.png"
                                                            elif "gif" in content_type:
                                                                filename = "image.gif"
                                                            elif "webp" in content_type:
                                                                filename = "image.webp"
                                                            else:
                                                                filename = "image.jpg"

                                if (
                                    image_data
                                    and len(image_data) > 0
                                    and len(image_data) <= 24 * 1024 * 1024
                                ):
                                    try:
                                        s3_session = aioboto3.Session()
                                        s3_data = {
                                            "service_name": "s3",
                                            "endpoint_url": environ.get("S3_URL"),
                                            "aws_access_key_id": environ.get(
                                                "S3_ACCESS"
                                            ),
                                            "aws_secret_access_key": environ.get(
                                                "S3_SECRET"
                                            ),
                                        }
                                        bucket_name = "5075293c-docs_generated"

                                        file_link = f"photos/messages_{db_message['id']}_{uuid4().hex[:8]}.{filename.split('.')[-1]}"

                                        async with s3_session.client(**s3_data) as s3:
                                            await s3.upload_fileobj(
                                                io.BytesIO(image_data),
                                                bucket_name,
                                                file_link,
                                            )

                                        image_url_for_db = file_link
                                        logger.info(f"File saved to S3: {file_link}")
                                    except Exception as e:
                                        logger.error(
                                            f"Failed to save file to S3: {e}",
                                            exc_info=True,
                                        )

                                if (
                                    message.message_type == "IMAGE"
                                    and image_data
                                    and len(image_data) > 0
                                    and len(image_data) <= 24 * 1024 * 1024
                                ):
                                    try:
                                        upload_result = await client.upload_image(
                                            image_data, filename
                                        )

                                        if upload_result:
                                            if isinstance(upload_result, tuple):
                                                image_id, avito_image_url = (
                                                    upload_result
                                                )
                                                if avito_image_url:
                                                    image_url_for_db = avito_image_url
                                            else:
                                                image_id = upload_result
                                        else:
                                            image_id = None
                                    except Exception as e:
                                        logger.error(
                                            f"Failed to upload image to Avito: {e}",
                                            exc_info=True,
                                        )
                                        image_id = None

                            except Exception as e:
                                logger.error(
                                    f"Failed to process image: {e}", exc_info=True
                                )
                                image_id = None

                        if (
                            message.message_type == "IMAGE" or (has_image and has_text)
                        ) and image_url_for_db:
                            try:
                                from database.db import pictures

                                await database.execute(
                                    pictures.insert().values(
                                        entity="messages",
                                        entity_id=db_message["id"],
                                        url=image_url_for_db,
                                        is_main=False,
                                        is_deleted=False,
                                        owner=user.id,
                                        cashbox=user.cashbox_id,
                                        size=len(image_data) if image_data else None,
                                    )
                                )
                                logger.info(
                                    f"File saved to pictures table for message {db_message['id']}: {image_url_for_db}"
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Failed to save file for message {db_message['id']}: {e}",
                                    exc_info=True,
                                )

                        if has_image and has_text and db_message_text:
                            if image_id:
                                try:
                                    avito_message_image = await client.send_message(
                                        chat_id=chat["external_chat_id"],
                                        text=None,
                                        image_id=image_id,
                                    )

                                    external_message_id = avito_message_image.get("id")
                                    if external_message_id:
                                        await crud.update_message(
                                            db_message["id"],
                                            external_message_id=external_message_id,
                                            status="DELIVERED",
                                        )
                                except Exception as e:
                                    logger.error(
                                        f"Failed to send IMAGE message to Avito: {e}",
                                        exc_info=True,
                                    )
                                    await crud.update_message(
                                        db_message["id"], status="FAILED"
                                    )

                            try:
                                avito_message_text = await client.send_message(
                                    chat_id=chat["external_chat_id"],
                                    text=message.content,
                                    image_id=None,
                                )

                                external_message_id = avito_message_text.get("id")
                                if external_message_id:
                                    await crud.update_message(
                                        db_message_text["id"],
                                        external_message_id=external_message_id,
                                        status="DELIVERED",
                                    )
                            except Exception as e:
                                logger.error(
                                    f"Failed to send TEXT message to Avito: {e}",
                                    exc_info=True,
                                )
                                await crud.update_message(
                                    db_message_text["id"], status="FAILED"
                                )
                        else:
                            send_image = message.message_type == "IMAGE" and image_id
                            send_text = message.content and message.content.strip()

                            if send_image or send_text:
                                try:
                                    avito_message = await client.send_message(
                                        chat_id=chat["external_chat_id"],
                                        text=message.content if send_text else None,
                                        image_id=image_id if send_image else None,
                                    )

                                    external_message_id = avito_message.get("id")
                                    if external_message_id:
                                        await crud.update_message(
                                            db_message["id"],
                                            external_message_id=external_message_id,
                                            status="DELIVERED",
                                        )
                                except Exception as e:
                                    logger.error(
                                        f"Failed to send message to Avito: {e}",
                                        exc_info=True,
                                    )
                                    await crud.update_message(
                                        db_message["id"], status="FAILED"
                                    )
                            else:
                                logger.warning(
                                    "Cannot send message: no image_id and no text content"
                                )
                                await crud.update_message(
                                    db_message["id"], status="FAILED"
                                )
                    except Exception as e:
                        logger.error(
                            f"Failed to send message to Avito: {e}", exc_info=True
                        )
                        try:
                            await crud.update_message(db_message["id"], status="FAILED")
                        except Exception:
                            pass
                else:
                    logger.warning(
                        f"Could not create Avito client for channel {channel['id']}, cashbox {user.cashbox_id}"
                    )
        except Exception as e:
            logger.error(f"Error sending message to Avito: {e}", exc_info=True)

    return db_message


@router.get("/messages/{message_id}", response_model=MessageResponse)
async def get_message(message_id: int, token: str, user=Depends(get_current_user)):
    """Get message by ID (must belong to user's cashbox)"""
    message = await crud.get_message(message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")

    chat = await crud.get_chat(message["chat_id"])
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    return message


@router.get("/messages/chat/{chat_id}", response_model=MessagesList)
async def get_chat_messages(
    chat_id: int,
    token: str,
    skip: int = 0,
    limit: int = 100,
    user=Depends(get_current_user),
):
    """Get messages from chat (must belong to user's cashbox)"""
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    messages = await crud.get_messages(chat_id, skip, limit)
    total = await crud.get_messages_count(chat_id)

    messages_list = []
    if messages:
        channel = await crud.get_channel(chat["channel_id"])
        client_avatar = (
            chat.get("contact", {}).get("avatar") if chat.get("contact") else None
        )
        operator_avatar = None

        if channel and channel["type"] == "AVITO":
            try:
                from api.chats.avito.avito_factory import (
                    create_avito_client,
                    save_token_callback,
                )
                from database.db import channel_credentials

                creds = await database.fetch_one(
                    channel_credentials.select().where(
                        (channel_credentials.c.channel_id == chat["channel_id"])
                        & (channel_credentials.c.cashbox_id == user.cashbox_id)
                        & (channel_credentials.c.is_active.is_(True))
                    )
                )

                if creds:
                    client = await create_avito_client(
                        channel_id=chat["channel_id"],
                        cashbox_id=user.cashbox_id,
                        on_token_refresh=lambda token_data: save_token_callback(
                            chat["channel_id"], user.cashbox_id, token_data
                        ),
                    )

                    if client:
                        chat_info = await client.get_chat_info(chat["external_chat_id"])
                        users = chat_info.get("users", [])
                        avito_user_id = creds.get("avito_user_id")

                        if users:
                            for user_data in users:
                                user_id_in_chat = user_data.get(
                                    "user_id"
                                ) or user_data.get("id")
                                if user_id_in_chat:
                                    avatar_url = None
                                    public_profile = user_data.get(
                                        "public_user_profile", {}
                                    )
                                    if public_profile:
                                        avatar_data = public_profile.get("avatar", {})
                                        if isinstance(avatar_data, dict):
                                            avatar_url = (
                                                avatar_data.get("default")
                                                or avatar_data.get("images", {}).get(
                                                    "256x256"
                                                )
                                                or avatar_data.get("images", {}).get(
                                                    "128x128"
                                                )
                                                or (
                                                    list(
                                                        avatar_data.get(
                                                            "images", {}
                                                        ).values()
                                                    )[0]
                                                    if avatar_data.get("images")
                                                    else None
                                                )
                                            )
                                        elif isinstance(avatar_data, str):
                                            avatar_url = avatar_data

                                    if avatar_url:
                                        if (
                                            avito_user_id
                                            and user_id_in_chat == avito_user_id
                                        ):
                                            operator_avatar = avatar_url
                                        elif not client_avatar:
                                            client_avatar = avatar_url
                                            # Сохраняем аватар в БД
                                            if chat.get("chat_contact_id"):
                                                from database.db import chat_contacts

                                                await database.execute(
                                                    chat_contacts.update()
                                                    .where(
                                                        chat_contacts.c.id
                                                        == chat["chat_contact_id"]
                                                    )
                                                    .values(avatar=avatar_url)
                                                )
            except Exception:
                pass

        message_ids = [msg["id"] for msg in messages] if messages else []
        message_images = {}
        if message_ids:
            try:
                from database.db import pictures
                from sqlalchemy import select

                query = (
                    select(pictures)
                    .where(
                        pictures.c.entity == "messages",
                        pictures.c.entity_id.in_(message_ids),
                        pictures.c.is_deleted.is_not(True),
                    )
                    .order_by(pictures.c.created_at.asc())
                )
                pictures_list = await database.fetch_all(query)

                for pic in pictures_list:
                    msg_id = pic["entity_id"]
                    if msg_id not in message_images:
                        message_images[msg_id] = []
                    pic_url = pic["url"]
                    if pic_url:
                        message_images[msg_id].append(pic_url)
            except Exception as e:
                logger.warning(
                    f"Failed to load images for messages: {e}", exc_info=True
                )

        for msg in messages:
            msg_dict = dict(msg)
            if msg_dict.get("sender_type") == "CLIENT":
                msg_dict["sender_avatar"] = client_avatar
            elif msg_dict.get("sender_type") == "OPERATOR":
                msg_dict["sender_avatar"] = operator_avatar
            else:
                msg_dict["sender_avatar"] = None

            msg_id = msg_dict.get("id")
            if msg_id in message_images and message_images[msg_id]:
                image_url = message_images[msg_id][0]
                if msg_dict.get("message_type") == "IMAGE":
                    msg_dict["image_url"] = image_url
                    msg_dict["file_url"] = image_url
                else:
                    msg_dict["file_url"] = image_url

            messages_list.append(MessageResponse(**msg_dict))

    return MessagesList(
        data=messages_list,
        total=total,
        skip=skip,
        limit=limit,
        date=chat.get("last_message_time"),
    )


@router.delete("/messages/{message_id}")
async def delete_message(message_id: int, token: str, user=Depends(get_current_user)):

    import logging

    logger = logging.getLogger(__name__)

    existing_message = await crud.get_message(message_id)
    if not existing_message:
        raise HTTPException(status_code=404, detail="Message not found")

    chat = await crud.get_chat(existing_message["chat_id"])
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    if existing_message.get("external_message_id") and chat.get("external_chat_id"):
        try:
            channel = await crud.get_channel(chat["channel_id"])

            if channel and channel["type"] == "AVITO":
                from api.chats.avito.avito_factory import (
                    create_avito_client,
                    save_token_callback,
                )

                client = await create_avito_client(
                    channel_id=channel["id"],
                    cashbox_id=user.cashbox_id,
                    on_token_refresh=lambda token_data: save_token_callback(
                        channel["id"], user.cashbox_id, token_data
                    ),
                )

                if client:
                    try:
                        deleted = await client.delete_message(
                            chat_id=chat["external_chat_id"],
                            message_id=existing_message["external_message_id"],
                        )
                        if deleted:
                            logger.info(f"Message {message_id} deleted in Avito API")
                        else:
                            logger.warning(
                                f"Failed to delete message {message_id} in Avito API"
                            )
                    except Exception as e:
                        logger.warning(f"Error deleting message in Avito API: {e}")
        except Exception as e:
            logger.warning(f"Error during Avito message deletion: {e}")

    return await crud.delete_message(message_id)


@router.get("/chats/{chat_id}/files/", response_model=list)
async def get_chat_files(chat_id: int, token: str, user=Depends(get_current_user)):
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    messages = await crud.get_messages(chat_id, skip=0, limit=1000)
    message_ids = [msg["id"] for msg in messages]

    if not message_ids:
        return []

    query = (
        select(pictures)
        .where(
            pictures.c.entity == "messages",
            pictures.c.entity_id.in_(message_ids),
            pictures.c.is_deleted.is_not(True),
        )
        .order_by(pictures.c.created_at.desc())
    )

    files = await database.fetch_all(query)
    return files


@router.get("/messages/{message_id}/files/", response_model=list)
async def get_message_files(
    message_id: int, token: str, user=Depends(get_current_user)
):
    message = await crud.get_message(message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")

    chat = await crud.get_chat(message["chat_id"])
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    query = (
        select(pictures)
        .where(
            pictures.c.entity == "messages",
            pictures.c.entity_id == message_id,
            pictures.c.is_deleted.is_not(True),
        )
        .order_by(pictures.c.created_at.desc())
    )

    files = await database.fetch_all(query)
    return files


@router.post("/chats/{chat_id}/chain_client/", response_model=dict)
async def chain_client_endpoint(
    chat_id: int,
    token: str,
    request: ChainClientRequest,
    message_id: Optional[int] = None,
    user=Depends(get_current_user),
):
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    return await crud.chain_client(
        chat_id=chat_id, message_id=message_id, phone=request.phone, name=request.name
    )


@router.get("/chats/{chat_id}/managers/", response_model=ManagersInChatResponse)
async def get_managers_in_chat(
    chat_id: int, token: str, user=Depends(get_current_user)
):
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    connected_users = chat_manager.get_connected_users(chat_id)

    managers = [
        ManagerInChat(
            user_id=user_info["user_id"],
            user_type=user_info["user_type"],
            connected_at=user_info["connected_at"],
        )
        for user_info in connected_users
        if user_info["user_type"] == "OPERATOR"
    ]

    return ManagersInChatResponse(
        chat_id=chat_id, managers=managers, total=len(managers)
    )

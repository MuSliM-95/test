import logging
from datetime import datetime
from typing import Optional

from api.chats import crud
from api.chats.auth import get_current_user, get_current_user_owner
from api.chats.producer import chat_producer
from api.chats.schemas import (
    ChainClientRequest,
    ChannelCreate,
    ChannelResponse,
    ChannelUpdate,
    ChatCreate,
    ChatResponse,
    ManagerInChat,
    ManagersInChatResponse,
    MessageCreate,
    MessageResponse,
    MessagesList,
)
from api.chats.telegram.telegram_handler import refresh_telegram_avatar
from api.chats.websocket import chat_manager
from common.utils.url_helper import get_app_url_for_environment
from database.db import chat_contacts, chat_messages, database, pictures
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import and_, select, update

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chats", tags=["chats"])


def _build_telegram_inline_keyboard(buttons):
    if not buttons:
        return None

    keyboard = []
    for row in buttons:
        row_buttons = []
        for button in row:
            data = {"text": button.text}
            if button.url:
                data["url"] = button.url
            else:
                data["callback_data"] = button.callback_data or button.text
            row_buttons.append(data)
        if row_buttons:
            keyboard.append(row_buttons)

    if not keyboard:
        return None

    return {"inline_keyboard": keyboard}


def _build_telegram_reply_keyboard(
    buttons, resize_keyboard=True, one_time_keyboard=False
):
    if not buttons:
        return None

    keyboard = []
    for row in buttons:
        row_buttons = []
        for button in row:
            data = {"text": button.text}
            if button.request_contact:
                data["request_contact"] = True
            if button.request_location:
                data["request_location"] = True
            row_buttons.append(data)
        if row_buttons:
            keyboard.append(row_buttons)

    if not keyboard:
        return None

    return {
        "keyboard": keyboard,
        "resize_keyboard": bool(resize_keyboard),
        "one_time_keyboard": bool(one_time_keyboard),
    }


def _parse_data_url(data_url: str):
    import base64

    if not data_url.startswith("data:") or "," not in data_url:
        return None

    header, encoded = data_url.split(",", 1)
    content_type = header.split(";")[0].split(":")[1] if ":" in header else None
    return base64.b64decode(encoded), content_type


def _is_placeholder_content(content: Optional[str]) -> bool:
    if not content:
        return False
    normalized = content.strip().lower()
    if normalized.startswith("[file"):
        return True
    if normalized.startswith("[doc"):
        return True
    if normalized.startswith("[document"):
        return True
    if normalized.startswith("[image"):
        return True
    return normalized in {
        "[photo]",
        "[document]",
        "[doc]",
        "[file]",
        "[image]",
        "[video]",
        "[voice]",
        "[message]",
        "photo",
        "file",
        "doc",
        "image",
        "document",
        "video",
        "voice",
        "message",
        "документ",
        "фото",
        "видео",
        "голосовое",
        "сообщение",
    }


async def _refresh_telegram_avatar_for_chat(chat: dict) -> None:
    if not chat or chat.get("channel_type") != "TELEGRAM":
        return
    contact = chat.get("contact") or {}
    if contact.get("avatar"):
        return
    contact_id = chat.get("chat_contact_id")
    if not contact_id:
        return

    contact_row = await database.fetch_one(
        chat_contacts.select().where(chat_contacts.c.id == contact_id)
    )
    if not contact_row:
        return

    external_contact_id = contact_row.get("external_contact_id")
    if not external_contact_id:
        external_contact_id = chat.get("external_chat_id")
    if not external_contact_id:
        return

    avatar_url = await refresh_telegram_avatar(
        channel_id=chat.get("channel_id"),
        cashbox_id=chat.get("cashbox_id"),
        external_contact_id=external_contact_id,
    )
    if not avatar_url:
        return

    await database.execute(
        chat_contacts.update()
        .where(chat_contacts.c.id == contact_id)
        .values(avatar=avatar_url, updated_at=datetime.utcnow())
    )
    contact["avatar"] = avatar_url
    chat["contact"] = contact


def _normalize_telegram_file_url(file_url: str) -> str:
    if not file_url:
        return file_url

    app_url = get_app_url_for_environment()
    if not app_url:
        return file_url

    if not app_url.startswith("http"):
        app_url = f"https://{app_url}"

    scheme, host = app_url.split("://", 1)
    if file_url.startswith("http://") or file_url.startswith("https://"):
        if "api.telegram.org/" in file_url:
            return file_url
        if host in file_url:
            normalized = file_url.split(host, 1)[-1].lstrip("/")
            if normalized.startswith(host):
                normalized = normalized.split(host, 1)[-1].lstrip("/")
            if normalized.startswith("api/v1/photos/"):
                return f"{scheme}://{host}/{normalized}"
            if normalized.startswith("photos/") or normalized.startswith(
                "chats_files/"
            ):
                return f"{scheme}://{host}/api/v1/photos/{normalized}"
        return file_url

    normalized = file_url.lstrip("/")
    if normalized.startswith(host):
        return f"{scheme}://{normalized}"
    if normalized.startswith("api/v1/photos/"):
        return f"{app_url.rstrip('/')}/{normalized}"
    if normalized.startswith("photos/") or normalized.startswith("chats_files/"):
        return f"{app_url.rstrip('/')}/api/v1/photos/{normalized}"
    return f"{app_url.rstrip('/')}/{normalized}"


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
    token: str,
    skip: int = 0,
    limit: int = 100,
    channel_type: Optional[str] = None,
    user=Depends(get_current_user),
):
    return await crud.get_all_channels_by_cashbox(user.cashbox_id, channel_type)


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

    try:
        await _refresh_telegram_avatar_for_chat(chat)
    except Exception as exc:
        logger.warning(
            "Failed to refresh Telegram avatar for chat %s: %s", chat_id, exc
        )

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
    chats = await crud.get_chats(
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
    for chat in chats:
        try:
            await _refresh_telegram_avatar_for_chat(chat)
        except Exception as exc:
            logger.warning(
                "Failed to refresh Telegram avatar for chat %s: %s",
                chat.get("id"),
                exc,
            )
    return chats


@router.post("/messages/", response_model=MessageResponse)
async def create_message(
    token: str, message: MessageCreate, user=Depends(get_current_user)
):
    """Create a new message"""
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
            content="",
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
                                        logger.error(f"Failed to parse data URL: {e}")
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
                                    except Exception as e:
                                        logger.error(f"Failed to save file to S3: {e}")

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
                                            f"Failed to upload image to Avito: {e}"
                                        )
                                        image_id = None

                            except Exception as e:
                                logger.error(f"Failed to process image: {e}")
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
                            except Exception as e:
                                logger.warning(
                                    f"Failed to save file for message {db_message['id']}: {e}"
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
                                        f"Failed to send IMAGE message to Avito: {e}"
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
                                    f"Failed to send TEXT message to Avito: {e}"
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
                                        f"Failed to send message to Avito: {e}"
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
                        logger.error(f"Failed to send message to Avito: {e}")
                        try:
                            await crud.update_message(db_message["id"], status="FAILED")
                        except Exception:
                            pass
                else:
                    logger.warning(
                        f"Could not create Avito client for channel {channel['id']}, cashbox {user.cashbox_id}"
                    )
            elif (
                channel
                and channel["type"] == "TELEGRAM"
                and chat.get("external_chat_id")
            ):
                try:
                    from api.chats.avito.avito_factory import _decrypt_credential
                    from api.chats.telegram.telegram_client import (
                        send_document,
                        send_media_group,
                        send_message,
                        send_photo,
                        send_video,
                    )
                    from database.db import channel_credentials, pictures

                    creds = await database.fetch_one(
                        channel_credentials.select().where(
                            (channel_credentials.c.channel_id == channel["id"])
                            & (channel_credentials.c.cashbox_id == user.cashbox_id)
                            & (channel_credentials.c.is_active.is_(True))
                        )
                    )

                    if not creds:
                        logger.warning(
                            f"No Telegram credentials for channel {channel['id']}, cashbox {user.cashbox_id}"
                        )
                        return db_message

                    bot_token = _decrypt_credential(creds["api_key"])
                    chat_id = chat["external_chat_id"]
                    if message.buttons_type == "reply":
                        keyboard = _build_telegram_reply_keyboard(
                            message.buttons,
                            resize_keyboard=message.buttons_resize,
                            one_time_keyboard=message.buttons_one_time,
                        )
                    else:
                        keyboard = _build_telegram_inline_keyboard(message.buttons)

                    file_urls = message.files or []
                    if message.image_url:
                        file_urls = [message.image_url] + file_urls

                    normalized_urls = []
                    for url in file_urls:
                        if isinstance(url, str) and url.startswith("data:"):
                            normalized_urls.append(url)
                        else:
                            normalized_urls.append(_normalize_telegram_file_url(url))
                    file_urls = normalized_urls

                    stored_urls = []
                    data_url_bytes = None
                    data_url_content_type = None

                    if len(file_urls) == 1 and file_urls[0].startswith("data:"):
                        parsed = _parse_data_url(file_urls[0])
                        if parsed:
                            data_url_bytes, data_url_content_type = parsed
                            try:
                                import io
                                import os
                                from uuid import uuid4

                                import aioboto3

                                extension = "bin"
                                if data_url_content_type:
                                    if "jpeg" in data_url_content_type:
                                        extension = "jpg"
                                    elif "png" in data_url_content_type:
                                        extension = "png"
                                    elif "gif" in data_url_content_type:
                                        extension = "gif"
                                    elif "pdf" in data_url_content_type:
                                        extension = "pdf"
                                s3_session = aioboto3.Session()
                                s3_data = {
                                    "service_name": "s3",
                                    "endpoint_url": os.environ.get("S3_URL"),
                                    "aws_access_key_id": os.environ.get("S3_ACCESS"),
                                    "aws_secret_access_key": os.environ.get(
                                        "S3_SECRET"
                                    ),
                                }
                                bucket_name = "5075293c-docs_generated"
                                file_link = f"photos/messages_{db_message['id']}_{uuid4().hex[:8]}.{extension}"

                                async with s3_session.client(**s3_data) as s3:
                                    await s3.upload_fileobj(
                                        io.BytesIO(data_url_bytes),
                                        bucket_name,
                                        file_link,
                                    )
                                stored_urls.append(file_link)
                            except Exception:
                                stored_urls.append(file_urls[0])
                        else:
                            stored_urls.append(file_urls[0])
                    else:
                        stored_urls = list(file_urls)

                    for url in stored_urls:
                        try:
                            await database.execute(
                                pictures.insert().values(
                                    entity="messages",
                                    entity_id=db_message["id"],
                                    url=url,
                                    is_main=False,
                                    is_deleted=False,
                                    owner=user.id,
                                    cashbox=user.cashbox_id,
                                )
                            )
                        except Exception:
                            pass

                    send_result = None
                    external_id = None
                    send_separate_text = bool(
                        has_image and has_text and db_message_text
                    )

                    if len(file_urls) > 1:
                        media_type = "photo"
                        if message.message_type == "VIDEO":
                            media_type = "video"
                        elif message.message_type == "DOCUMENT":
                            media_type = "document"

                        media_payload = []
                        for idx, url in enumerate(file_urls):
                            media_item = {"type": media_type, "media": url}
                            if idx == 0 and message.content:
                                media_item["caption"] = message.content
                            media_payload.append(media_item)

                        send_result = await send_media_group(
                            bot_token, chat_id, media_payload
                        )

                        if keyboard and send_separate_text and db_message_text:
                            text_result = await send_message(
                                bot_token,
                                chat_id,
                                message.content or " ",
                                reply_markup=keyboard,
                            )
                            if text_result:
                                await crud.update_message(
                                    db_message_text["id"],
                                    external_message_id=str(
                                        text_result.get("message_id")
                                    ),
                                    status="DELIVERED",
                                )

                        if send_result:
                            external_id = str(send_result[0].get("message_id"))
                    else:
                        file_payload = file_urls[0] if file_urls else None
                        if data_url_bytes is not None:
                            file_payload = data_url_bytes

                        if send_separate_text and file_payload:
                            send_result = await send_photo(
                                bot_token,
                                chat_id,
                                file_payload,
                                caption=None,
                            )
                            if send_result:
                                external_id = str(send_result.get("message_id"))

                            text_result = await send_message(
                                bot_token,
                                chat_id,
                                message.content,
                                reply_markup=keyboard,
                            )
                            if text_result and db_message_text:
                                await crud.update_message(
                                    db_message_text["id"],
                                    external_message_id=str(
                                        text_result.get("message_id")
                                    ),
                                    status="DELIVERED",
                                )
                        elif message.message_type == "IMAGE" and file_payload:
                            send_result = await send_photo(
                                bot_token,
                                chat_id,
                                file_payload,
                                caption=message.content,
                                reply_markup=keyboard,
                            )
                        elif message.message_type == "VIDEO" and file_payload:
                            send_result = await send_video(
                                bot_token,
                                chat_id,
                                file_payload,
                                caption=message.content,
                                reply_markup=keyboard,
                            )
                        elif message.message_type == "DOCUMENT" and file_payload:
                            send_result = await send_document(
                                bot_token,
                                chat_id,
                                file_payload,
                                caption=message.content,
                                reply_markup=keyboard,
                            )
                        else:
                            send_result = await send_message(
                                bot_token,
                                chat_id,
                                message.content,
                                reply_markup=keyboard,
                            )

                        if send_result:
                            external_id = str(send_result.get("message_id"))

                    if external_id:
                        await crud.update_message(
                            db_message["id"],
                            external_message_id=external_id,
                            status="DELIVERED",
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to send message to Telegram: {e}", exc_info=True
                    )
                    try:
                        await crud.update_message(db_message["id"], status="FAILED")
                    except Exception:
                        pass
        except Exception as e:
            logger.error(f"Error sending message to Avito: {e}")

    try:
        preview_url = None
        if message.image_url:
            preview_url = message.image_url
        elif message.files:
            preview_url = message.files[0]

        if preview_url and isinstance(preview_url, str):
            if not preview_url.startswith("data:"):
                preview_url = _normalize_telegram_file_url(preview_url)
            else:
                preview_url = None

        if _is_placeholder_content(db_message.get("content")) and preview_url:
            await crud.update_message(db_message["id"], content=preview_url)
            db_message["content"] = preview_url

        await chat_producer.send_message(
            chat["id"],
            {
                "message_id": db_message["id"],
                "chat_id": chat["id"],
                "sender_type": db_message["sender_type"],
                "content": db_message.get("content") or "",
                "message_type": db_message.get("message_type") or "TEXT",
                "timestamp": datetime.utcnow().isoformat(),
            },
        )

        if db_message_text:
            await chat_producer.send_message(
                chat["id"],
                {
                    "message_id": db_message_text["id"],
                    "chat_id": chat["id"],
                    "sender_type": db_message_text["sender_type"],
                    "content": db_message_text.get("content") or "",
                    "message_type": db_message_text.get("message_type") or "TEXT",
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )
    except Exception as e:
        logger.warning(f"Failed to publish chat message event: {e}")

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
    background_tasks: BackgroundTasks = BackgroundTasks(),
    user=Depends(get_current_user),
):
    """Get messages from chat (must belong to user's cashbox)"""
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    channel = await crud.get_channel(chat["channel_id"])

    async def mark_chat_as_read_background():
        if channel and channel.get("type") == "AVITO" and chat.get("external_chat_id"):
            try:
                from api.chats.avito.avito_factory import (
                    create_avito_client,
                    save_token_callback,
                )

                client = await create_avito_client(
                    channel_id=chat["channel_id"],
                    cashbox_id=user.cashbox_id,
                    on_token_refresh=lambda token_data: save_token_callback(
                        chat["channel_id"], user.cashbox_id, token_data
                    ),
                )

                if client:
                    try:
                        await client.mark_chat_as_read(chat["external_chat_id"])

                        update_query = (
                            update(chat_messages)
                            .where(
                                and_(
                                    chat_messages.c.chat_id == chat_id,
                                    chat_messages.c.sender_type == "CLIENT",
                                    chat_messages.c.status != "READ",
                                )
                            )
                            .values(status="READ")
                        )
                        await database.execute(update_query)
                    except Exception as e:
                        logger.warning(
                            f"Failed to mark chat {chat['external_chat_id']} as read: {e}"
                        )
            except Exception as e:
                logger.warning(f"Failed to mark Avito chat {chat_id} as read: {e}")

    if channel and channel.get("type") == "AVITO" and chat.get("external_chat_id"):
        background_tasks.add_task(mark_chat_as_read_background)

    messages = await crud.get_messages(chat_id, skip, limit)
    total = await crud.get_messages_count(chat_id)

    messages_list = []
    if messages:
        channel = await crud.get_channel(chat["channel_id"])
        client_avatar = (
            chat.get("contact", {}).get("avatar") if chat.get("contact") else None
        )
        operator_avatar = None

        if channel and channel["type"] == "AVITO" and chat.get("external_chat_id"):
            needs_avatars = not client_avatar

            if needs_avatars:
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
                            chat_info = await client.get_chat_info(
                                chat["external_chat_id"]
                            )
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
                                            avatar_data = public_profile.get(
                                                "avatar", {}
                                            )
                                            if isinstance(avatar_data, dict):
                                                avatar_url = (
                                                    avatar_data.get("default")
                                                    or avatar_data.get(
                                                        "images", {}
                                                    ).get("256x256")
                                                    or avatar_data.get(
                                                        "images", {}
                                                    ).get("128x128")
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
                                                if chat.get("chat_contact_id"):
                                                    from database.db import (
                                                        chat_contacts,
                                                    )

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

        if client_avatar:
            client_avatar = _normalize_telegram_file_url(client_avatar)
        if operator_avatar:
            operator_avatar = _normalize_telegram_file_url(operator_avatar)

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
                        message_images[msg_id].append(
                            _normalize_telegram_file_url(pic_url)
                        )
            except Exception as e:
                logger.warning(f"Failed to load images for messages: {e}")

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

    result = MessagesList(
        data=messages_list,
        total=total,
        skip=skip,
        limit=limit,
        date=chat.get("last_message_time"),
    )

    return result


@router.delete("/messages/{message_id}")
async def delete_message(message_id: int, token: str, user=Depends(get_current_user)):
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
                        await client.delete_message(
                            chat_id=chat["external_chat_id"],
                            message_id=existing_message["external_message_id"],
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

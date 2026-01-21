import io
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from uuid import uuid4

import aioboto3
from api.chats import crud
from api.chats.producer import chat_producer
from api.chats.telegram.telegram_client import (
    answer_callback_query,
    download_file,
    get_file,
    get_user_profile_photos,
)
from common.utils.url_helper import get_app_url_for_environment
from database.db import cboxes, chat_messages, chats, database, pictures

logger = logging.getLogger(__name__)

_MEDIA_GROUP_CACHE: Dict[str, Dict[str, Any]] = {}
_MEDIA_GROUP_TTL = timedelta(minutes=10)
_AVATAR_CACHE: Dict[str, str] = {}
_CASHBOX_OWNER_CACHE: Dict[int, int] = {}


def _cleanup_media_groups() -> None:
    now = datetime.utcnow()
    expired = [
        key
        for key, value in _MEDIA_GROUP_CACHE.items()
        if now - value["created_at"] > _MEDIA_GROUP_TTL
    ]
    for key in expired:
        _MEDIA_GROUP_CACHE.pop(key, None)


def _build_contact_name(user: Dict[str, Any]) -> Optional[str]:
    first = user.get("first_name") or ""
    last = user.get("last_name") or ""
    username = user.get("username")
    full = f"{first} {last}".strip()
    if full:
        return full
    if username:
        return username
    return None


def _build_chat_name(user: Dict[str, Any], chat_external_id: str) -> str:
    username = user.get("username")
    if username:
        return f"Telegram @{username} ({chat_external_id})"
    return f"Telegram {chat_external_id}"


def _normalize_app_url(app_url: Optional[str]) -> Optional[str]:
    if not app_url:
        return None
    if not app_url.startswith("http://") and not app_url.startswith("https://"):
        app_url = f"https://{app_url}"
    return app_url.rstrip("/")


def _build_public_photo_url(file_key: str) -> Optional[str]:
    app_url = _normalize_app_url(get_app_url_for_environment())
    if not app_url:
        return None
    return f"{app_url}/api/v1/photos/{file_key}"


async def _get_cashbox_owner_id(cashbox_id: int) -> Optional[int]:
    cached = _CASHBOX_OWNER_CACHE.get(cashbox_id)
    if cached is not None:
        return cached
    row = await database.fetch_one(cboxes.select().where(cboxes.c.id == cashbox_id))
    owner_id = row["admin"] if row else None
    if owner_id is not None:
        _CASHBOX_OWNER_CACHE[cashbox_id] = owner_id
    return owner_id


def _guess_extension(filename: Optional[str], mime_type: Optional[str]) -> str:
    if filename and "." in filename:
        return filename.rsplit(".", 1)[-1].lower()
    if mime_type:
        if "jpeg" in mime_type:
            return "jpg"
        if "png" in mime_type:
            return "png"
        if "gif" in mime_type:
            return "gif"
        if "pdf" in mime_type:
            return "pdf"
        if "mp4" in mime_type:
            return "mp4"
        if "ogg" in mime_type or "opus" in mime_type:
            return "ogg"
    return "bin"


def _guess_extension_from_bytes(file_bytes: bytes) -> Optional[str]:
    if not file_bytes:
        return None
    head = file_bytes[:16]
    if head.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if head.startswith(b"GIF87a") or head.startswith(b"GIF89a"):
        return "gif"
    if head.startswith(b"%PDF-"):
        return "pdf"
    if head.startswith(b"OggS"):
        return "ogg"
    if len(head) >= 12 and head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "webp"
    if len(head) >= 12 and head[:4] == b"RIFF" and head[8:12] == b"WAVE":
        return "wav"
    if len(head) >= 8 and head[4:8] == b"ftyp":
        return "mp4"
    if head.startswith(b"ID3") or head[:2] == b"\xff\xfb":
        return "mp3"
    return None


def _extract_filename(file_path: Optional[str]) -> Optional[str]:
    if not file_path:
        return None
    return file_path.rsplit("/", 1)[-1]


async def _upload_to_s3(
    file_bytes: bytes,
    message_id: int,
    extension: str,
    cashbox_id: int,
    channel_id: int,
) -> Optional[str]:
    s3_session = aioboto3.Session()
    s3_data = {
        "service_name": "s3",
        "endpoint_url": os.environ.get("S3_URL"),
        "aws_access_key_id": os.environ.get("S3_ACCESS"),
        "aws_secret_access_key": os.environ.get("S3_SECRET"),
    }
    bucket_name = "5075293c-docs_generated"

    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    file_link = (
        f"chats_files/{cashbox_id}/{channel_id}/{date_path}/"
        f"message_{message_id}_{uuid4().hex[:8]}.{extension}"
    )

    async with s3_session.client(**s3_data) as s3:
        await s3.upload_fileobj(io.BytesIO(file_bytes), bucket_name, file_link)

    return file_link


async def _upload_avatar_to_s3(
    file_bytes: bytes,
    user_id: str,
    extension: str,
    cashbox_id: int,
    channel_id: int,
) -> Optional[str]:
    s3_session = aioboto3.Session()
    s3_data = {
        "service_name": "s3",
        "endpoint_url": os.environ.get("S3_URL"),
        "aws_access_key_id": os.environ.get("S3_ACCESS"),
        "aws_secret_access_key": os.environ.get("S3_SECRET"),
    }
    bucket_name = "5075293c-docs_generated"

    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    file_link = (
        f"chats_files/{cashbox_id}/{channel_id}/{date_path}/"
        f"avatar_{user_id}_{uuid4().hex[:8]}.{extension}"
    )

    async with s3_session.client(**s3_data) as s3:
        await s3.upload_fileobj(io.BytesIO(file_bytes), bucket_name, file_link)

    return file_link


async def _get_avatar_url(
    user_id: Optional[int],
    bot_token: str,
    cashbox_id: int,
    channel_id: int,
) -> Optional[str]:
    if not user_id:
        return None

    user_key = str(user_id)
    cached = _AVATAR_CACHE.get(user_key)
    if cached:
        return cached

    try:
        profile = await get_user_profile_photos(bot_token, user_id, limit=1)
    except Exception:
        return None

    photos = profile.get("photos") if isinstance(profile, dict) else None
    if not photos:
        return None

    sizes = photos[0] if photos else []
    if not sizes:
        return None

    best = max(
        sizes,
        key=lambda item: item.get("file_size")
        or (item.get("width", 0) * item.get("height", 0)),
    )
    file_id = best.get("file_id")
    if not file_id:
        return None

    try:
        file_meta = await get_file(bot_token, file_id)
        file_path = file_meta.get("file_path")
        if not file_path:
            return None
        file_bytes = await download_file(bot_token, file_path)
        filename = _extract_filename(file_path)
        extension = _guess_extension(filename, None)
        if extension == "bin":
            extension = _guess_extension_from_bytes(file_bytes) or "bin"
        file_link = await _upload_avatar_to_s3(
            file_bytes=file_bytes,
            user_id=user_key,
            extension=extension,
            cashbox_id=cashbox_id,
            channel_id=channel_id,
        )
    except Exception:
        return None

    if not file_link:
        return None

    avatar_url = _build_public_photo_url(file_link) or file_link

    _AVATAR_CACHE[user_key] = avatar_url
    return avatar_url


async def _store_picture(
    message_id: int,
    file_url: str,
    cashbox_id: int,
    size: Optional[int] = None,
) -> None:
    await database.execute(
        pictures.insert().values(
            entity="messages",
            entity_id=message_id,
            url=file_url,
            is_main=False,
            is_deleted=False,
            owner=cashbox_id,
            cashbox=cashbox_id,
            size=size,
        )
    )


async def _ensure_chat(
    channel_id: int,
    cashbox_id: int,
    external_chat_id: str,
    external_contact_id: Optional[str],
    name: Optional[str],
    chat_name: Optional[str],
    metadata: Optional[Dict[str, Any]],
    avatar: Optional[str] = None,
) -> Dict[str, Any]:
    chat = await crud.get_chat_by_external_id(
        channel_id=channel_id, external_chat_id=external_chat_id, cashbox_id=cashbox_id
    )
    if chat:
        if (
            avatar
            and (not chat.get("contact") or not chat["contact"].get("avatar"))
            and external_contact_id
        ):
            await crud.get_or_create_chat_contact(
                channel_id=channel_id,
                external_contact_id=external_contact_id,
                name=name,
                avatar=avatar,
            )
        if chat_name:
            existing_meta = chat.get("metadata") or {}
            if isinstance(existing_meta, str):
                try:
                    existing_meta = json.loads(existing_meta)
                except Exception:
                    existing_meta = {}
            if isinstance(existing_meta, dict) and not existing_meta.get("ad_title"):
                updated_meta = dict(existing_meta)
                updated_meta["ad_title"] = chat_name
                await database.execute(
                    chats.update()
                    .where(chats.c.id == chat["id"])
                    .values(metadata=updated_meta, updated_at=datetime.utcnow())
                )
        return chat

    if chat_name:
        metadata = dict(metadata or {})
        metadata.setdefault("ad_title", chat_name)

    chat = await crud.create_chat(
        channel_id=channel_id,
        cashbox_id=cashbox_id,
        external_chat_id=external_chat_id,
        external_chat_id_for_contact=external_contact_id,
        name=name,
        avatar=avatar,
        metadata=metadata,
    )
    return chat


async def handle_update(
    update: Dict[str, Any],
    channel_id: int,
    cashbox_id: int,
    bot_token: str,
) -> Dict[str, Any]:
    _cleanup_media_groups()

    callback_query = update.get("callback_query")
    if callback_query:
        message = callback_query.get("message") or {}
        chat_info = message.get("chat") or {}
        user_info = callback_query.get("from") or {}
        chat_external_id = str(chat_info.get("id") or "")

        if not chat_external_id:
            return {"success": True, "message": "No chat id"}

        avatar_url = await _get_avatar_url(
            user_info.get("id"),
            bot_token,
            cashbox_id=cashbox_id,
            channel_id=channel_id,
        )

        contact_name = _build_contact_name(user_info)
        chat_name = _build_chat_name(user_info, chat_external_id)
        chat = await _ensure_chat(
            channel_id=channel_id,
            cashbox_id=cashbox_id,
            external_chat_id=chat_external_id,
            external_contact_id=(
                str(user_info.get("id")) if user_info.get("id") else None
            ),
            name=contact_name,
            chat_name=chat_name,
            metadata={"source": "telegram", "username": user_info.get("username")},
            avatar=avatar_url,
        )

        content = callback_query.get("data") or "[Button]"
        existing = await database.fetch_one(
            chat_messages.select().where(
                (chat_messages.c.external_message_id == callback_query.get("id"))
                & (chat_messages.c.chat_id == chat["id"])
            )
        )
        if not existing:
            message_db = await crud.create_message_and_update_chat(
                chat_id=chat["id"],
                sender_type="CLIENT",
                content=content,
                message_type="SYSTEM",
                external_message_id=callback_query.get("id"),
                status="DELIVERED",
                source="telegram",
            )

            await chat_producer.send_message(
                chat["id"],
                {
                    "message_id": message_db["id"],
                    "chat_id": chat["id"],
                    "channel_type": "TELEGRAM",
                    "external_message_id": callback_query.get("id"),
                    "sender_type": "CLIENT",
                    "content": content,
                    "message_type": "SYSTEM",
                    "created_at": datetime.utcnow().isoformat(),
                },
            )

        try:
            await answer_callback_query(bot_token, callback_query.get("id"), text="OK")
        except Exception:
            pass

        return {"success": True, "message": "Callback processed"}

    message = update.get("message") or update.get("edited_message")
    if not message:
        return {"success": True, "message": "No supported update"}

    chat_info = message.get("chat") or {}
    user_info = message.get("from") or {}
    chat_external_id = str(chat_info.get("id") or "")
    if not chat_external_id:
        return {"success": True, "message": "No chat id"}

    metadata = {
        "source": "telegram",
        "username": user_info.get("username"),
        "chat_title": chat_info.get("title"),
        "chat_type": chat_info.get("type"),
    }

    avatar_url = await _get_avatar_url(
        user_info.get("id"),
        bot_token,
        cashbox_id=cashbox_id,
        channel_id=channel_id,
    )

    contact_name = _build_contact_name(user_info)
    chat_name = _build_chat_name(user_info, chat_external_id)
    chat = await _ensure_chat(
        channel_id=channel_id,
        cashbox_id=cashbox_id,
        external_chat_id=chat_external_id,
        external_contact_id=str(user_info.get("id")) if user_info.get("id") else None,
        name=contact_name,
        chat_name=chat_name,
        metadata=metadata,
        avatar=avatar_url,
    )

    sender_type = "OPERATOR" if user_info.get("is_bot") else "CLIENT"

    message_text = message.get("text") or message.get("caption")
    message_type = "TEXT"
    file_info: Optional[Dict[str, Any]] = None

    if message.get("photo"):
        message_type = "IMAGE"
        photo_sizes = message["photo"]
        file_info = max(
            photo_sizes, key=lambda item: item.get("file_size") or item.get("width", 0)
        )
    elif message.get("document"):
        message_type = "DOCUMENT"
        file_info = message["document"]
    elif message.get("video"):
        message_type = "VIDEO"
        file_info = message["video"]
    elif message.get("voice"):
        message_type = "VOICE"
        file_info = message["voice"]
    elif message.get("sticker"):
        message_type = "SYSTEM"

    if not message_text:
        if message_type == "IMAGE":
            message_text = "[Photo]"
        elif message_type == "DOCUMENT":
            message_text = "[Document]"
        elif message_type == "VIDEO":
            message_text = "[Video]"
        elif message_type == "VOICE":
            message_text = "[Voice]"
        else:
            message_text = "[Message]"

    external_message_id = str(message.get("message_id") or "")

    if external_message_id:
        existing = await database.fetch_one(
            chat_messages.select().where(
                (chat_messages.c.external_message_id == external_message_id)
                & (chat_messages.c.chat_id == chat["id"])
            )
        )
        if existing:
            return {
                "success": True,
                "message": "Message already exists",
                "chat_id": chat["id"],
                "message_id": existing["id"],
            }

    media_group_id = message.get("media_group_id")
    group_key = None
    if media_group_id:
        group_key = f"{channel_id}:{chat_external_id}:{media_group_id}"

    message_db = None
    created_at = None
    if message.get("date"):
        try:
            created_at = datetime.utcfromtimestamp(message["date"])
        except Exception:
            created_at = None
    if group_key and group_key in _MEDIA_GROUP_CACHE:
        message_db = await crud.get_message(_MEDIA_GROUP_CACHE[group_key]["message_id"])
    else:
        message_db = await crud.create_message_and_update_chat(
            chat_id=chat["id"],
            sender_type=sender_type,
            content=message_text,
            message_type=message_type,
            external_message_id=external_message_id or None,
            status="DELIVERED",
            created_at=created_at,
            source="telegram",
        )
        if group_key:
            _MEDIA_GROUP_CACHE[group_key] = {
                "message_id": message_db["id"],
                "created_at": datetime.utcnow(),
            }

        await chat_producer.send_message(
            chat["id"],
            {
                "message_id": message_db["id"],
                "chat_id": chat["id"],
                "channel_type": "TELEGRAM",
                "external_message_id": external_message_id,
                "sender_type": sender_type,
                "content": message_text,
                "message_type": message_type,
                "created_at": datetime.utcnow().isoformat(),
            },
        )

    if file_info and message_db:
        file_id = file_info.get("file_id")
        if file_id:
            try:
                file_meta = await get_file(bot_token, file_id)
                file_path = file_meta.get("file_path")
                if file_path:
                    file_bytes = await download_file(bot_token, file_path)
                    filename = file_info.get("file_name")
                    mime_type = file_info.get("mime_type")
                    extension = _guess_extension(filename, mime_type)
                    if extension == "bin":
                        extension = _guess_extension_from_bytes(file_bytes) or "bin"
                    file_link = await _upload_to_s3(
                        file_bytes=file_bytes,
                        message_id=message_db["id"],
                        extension=extension,
                        cashbox_id=cashbox_id,
                        channel_id=channel_id,
                    )
                    if file_link:
                        public_url = _build_public_photo_url(file_link) or file_link
                        await _store_picture(
                            message_id=message_db["id"],
                            file_url=file_link,
                            cashbox_id=cashbox_id,
                            size=len(file_bytes),
                        )
                        if message_text.startswith("[") and message_text.endswith("]"):
                            await database.execute(
                                chat_messages.update()
                                .where(chat_messages.c.id == message_db["id"])
                                .values(
                                    content=public_url, updated_at=datetime.utcnow()
                                )
                            )
                            message_text = public_url
            except Exception as e:
                logger.warning(
                    f"Failed to store Telegram file for message {message_db['id']}: {e}",
                    exc_info=True,
                )

    if message.get("contact"):
        contact = message["contact"]
        phone = contact.get("phone_number")
        if phone:
            try:
                await crud.update_chat(
                    chat["id"],
                    phone=phone,
                    name=contact.get("first_name") or contact.get("last_name"),
                )
            except Exception:
                pass

    return {"success": True, "message": "Telegram update processed"}

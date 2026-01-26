import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from api.chats import crud
from api.chats.auth import get_current_user
from api.chats.producer import chat_producer
from common.utils.url_helper import get_app_url_for_environment
from database.db import channel_credentials, chat_messages, database, pictures
from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect

router = APIRouter(prefix="/ws", tags=["chats-ws"])
logger = logging.getLogger(__name__)


def _normalize_telegram_file_url(file_url: Optional[str]) -> Optional[str]:
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
    if normalized.startswith("api/v1/photos/"):
        return f"{app_url.rstrip('/')}/{normalized}"
    if normalized.startswith("photos/") or normalized.startswith("chats_files/"):
        return f"{app_url.rstrip('/')}/api/v1/photos/{normalized}"
    return f"{app_url.rstrip('/')}/{normalized}"


async def _is_duplicate_message(
    chat_id: int,
    sender_type: str,
    message_type: str,
    content: str,
    window_seconds: int = 5,
) -> bool:
    if not content:
        return False

    last_message = await database.fetch_one(
        chat_messages.select()
        .where(
            (chat_messages.c.chat_id == chat_id)
            & (chat_messages.c.sender_type == sender_type)
            & (chat_messages.c.message_type == message_type)
        )
        .order_by(chat_messages.c.created_at.desc())
        .limit(1)
    )

    if not last_message:
        return False

    last_content = (last_message["content"] or "").strip()
    if last_content != content.strip():
        return False

    last_created = last_message.get("created_at")
    if not last_created:
        return False

    delta = datetime.utcnow() - last_created
    return delta.total_seconds() <= window_seconds


@dataclass
class ChatConnectionInfo:
    websocket: WebSocket
    user_id: int
    user_type: str
    connected_at: datetime


class ChatConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, List[ChatConnectionInfo]] = {}

    async def connect(
        self, chat_id: int, websocket: WebSocket, user_id: int, user_type: str
    ):
        if chat_id not in self.active_connections:
            self.active_connections[chat_id] = []

        connection_info = ChatConnectionInfo(
            websocket=websocket,
            user_id=user_id,
            user_type=user_type,
            connected_at=datetime.utcnow(),
        )

        self.active_connections[chat_id].append(connection_info)

    async def disconnect(
        self, chat_id: int, websocket: WebSocket
    ) -> Optional[ChatConnectionInfo]:
        if chat_id in self.active_connections:
            connection_info = None
            for conn_info in self.active_connections[chat_id]:
                if conn_info.websocket == websocket:
                    connection_info = conn_info
                    self.active_connections[chat_id].remove(conn_info)
                    break

            if not self.active_connections[chat_id]:
                del self.active_connections[chat_id]

            if connection_info:
                return connection_info
            else:
                return None
        else:
            return None

    def get_connection_info(
        self, chat_id: int, websocket: WebSocket
    ) -> Optional[ChatConnectionInfo]:
        if chat_id in self.active_connections:
            for conn_info in self.active_connections[chat_id]:
                if conn_info.websocket == websocket:
                    return conn_info
        return None

    def get_connected_users(self, chat_id: int) -> List[Dict]:
        if chat_id not in self.active_connections:
            return []

        users = []
        for conn_info in self.active_connections[chat_id]:
            users.append(
                {
                    "user_id": conn_info.user_id,
                    "user_type": conn_info.user_type,
                    "connected_at": conn_info.connected_at.isoformat(),
                }
            )

        return users

    async def broadcast_to_chat(self, chat_id: int, message: dict):
        if chat_id in self.active_connections:
            disconnected_clients = []
            for i, conn_info in enumerate(self.active_connections[chat_id]):
                try:
                    await conn_info.websocket.send_json(message)
                except Exception as e:
                    disconnected_clients.append(i)

            for i in reversed(disconnected_clients):
                try:
                    self.active_connections[chat_id].pop(i)
                except Exception:
                    pass


chat_manager = ChatConnectionManager()


@dataclass
class CashboxConnectionInfo:
    websocket: WebSocket
    user_id: int
    cashbox_id: int
    connected_at: datetime


class CashboxConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, List[CashboxConnectionInfo]] = {}

    async def connect(self, cashbox_id: int, websocket: WebSocket, user_id: int):
        if cashbox_id not in self.active_connections:
            self.active_connections[cashbox_id] = []

        connection_info = CashboxConnectionInfo(
            websocket=websocket,
            user_id=user_id,
            cashbox_id=cashbox_id,
            connected_at=datetime.utcnow(),
        )

        self.active_connections[cashbox_id].append(connection_info)

    async def disconnect(
        self, cashbox_id: int, websocket: WebSocket
    ) -> Optional[CashboxConnectionInfo]:
        if cashbox_id in self.active_connections:
            connection_info = None
            for conn_info in self.active_connections[cashbox_id]:
                if conn_info.websocket == websocket:
                    connection_info = conn_info
                    self.active_connections[cashbox_id].remove(conn_info)
                    break

            if not self.active_connections[cashbox_id]:
                del self.active_connections[cashbox_id]

            return connection_info
        return None

    async def broadcast_to_cashbox(self, cashbox_id: int, message: dict):
        if cashbox_id in self.active_connections:
            disconnected_clients = []
            for i, conn_info in enumerate(self.active_connections[cashbox_id]):
                try:
                    await conn_info.websocket.send_json(message)
                except Exception as e:
                    disconnected_clients.append(i)

            for i in reversed(disconnected_clients):
                try:
                    self.active_connections[cashbox_id].pop(i)
                except Exception:
                    pass


cashbox_manager = CashboxConnectionManager()


@router.websocket("/chats/all/")
async def websocket_all_chats(websocket: WebSocket, token: str = Query(...)):
    try:
        await websocket.accept()
    except Exception as e:
        return

    cashbox_id = None
    try:
        try:
            user = await get_current_user(token)
        except HTTPException as e:
            error_detail = e.detail if hasattr(e, "detail") else str(e)
            try:
                await websocket.send_json(
                    {
                        "error": "Unauthorized",
                        "detail": error_detail,
                        "status_code": e.status_code,
                    }
                )
                await websocket.close(code=1008)
            except Exception:
                pass
            return
        except Exception as e:
            try:
                await websocket.send_json({"error": "Unauthorized", "detail": str(e)})
                await websocket.close(code=1008)
            except Exception:
                pass
            return

        cashbox_id = user.cashbox_id
        await cashbox_manager.connect(cashbox_id, websocket, user.user)

        try:
            await websocket.send_json(
                {
                    "type": "connected",
                    "cashbox_id": cashbox_id,
                    "user_id": user.user,
                    "message": "Successfully connected to all chats",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )
        except Exception as e:
            pass

        while True:
            try:
                data = await websocket.receive_text()
                message_data = json.loads(data)
            except WebSocketDisconnect:
                raise
            except json.JSONDecodeError:
                continue
            except Exception as e:
                continue

    except WebSocketDisconnect:
        if cashbox_id is not None:
            try:
                await cashbox_manager.disconnect(cashbox_id, websocket)
            except Exception:
                pass
    except Exception as e:
        if cashbox_id is not None:
            try:
                await cashbox_manager.disconnect(cashbox_id, websocket)
            except Exception:
                pass


@router.websocket("/chats/{chat_id}/")
async def websocket_chat(chat_id: int, websocket: WebSocket, token: str = Query(...)):
    await websocket.accept()

    try:
        try:
            user = await get_current_user(token)
        except HTTPException as e:
            error_detail = e.detail if hasattr(e, "detail") else str(e)
            await websocket.send_json(
                {
                    "error": "Unauthorized",
                    "detail": error_detail,
                    "status_code": e.status_code,
                }
            )
            await websocket.close(code=1008)
            return
        except Exception as e:
            await websocket.send_json({"error": "Unauthorized", "detail": str(e)})
            await websocket.close(code=1008)
            return

        chat = await crud.get_chat(chat_id)
        if not chat:
            await websocket.send_json({"error": "Chat not found", "chat_id": chat_id})
            await websocket.close(code=1008)
            return

        chat_cashbox_id = (
            chat.get("cashbox_id") if isinstance(chat, dict) else chat.cashbox_id
        )

        if chat_cashbox_id != user.cashbox_id:
            await websocket.send_json(
                {
                    "error": "Access denied",
                    "detail": "Chat belongs to different cashbox",
                    "chat_cashbox_id": chat_cashbox_id,
                    "user_cashbox_id": user.cashbox_id,
                }
            )
            await websocket.close(code=1008)
            return

        user_type = "OPERATOR" if user.is_owner else "OPERATOR"

        await chat_manager.connect(chat_id, websocket, user.user, user_type)

        try:
            await chat_producer.send_user_connected_event(chat_id, user.user, user_type)
        except Exception as e:
            pass

        try:
            await websocket.send_json(
                {
                    "type": "connected",
                    "chat_id": chat_id,
                    "user_id": user.user,
                    "user_type": user_type,
                    "message": "Successfully connected to chat",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )
        except Exception as e:
            pass

        while True:
            try:
                data = await websocket.receive_text()
                message_data = json.loads(data)

                event_type = message_data.get("type", "message")
            except WebSocketDisconnect:
                raise
            except json.JSONDecodeError as e:
                await websocket.send_json({"error": "Invalid JSON", "detail": str(e)})
                continue
            except Exception as e:
                try:
                    await websocket.send_json(
                        {"error": "Failed to process message", "detail": str(e)}
                    )
                except:
                    pass
                continue

            if event_type == "message":
                sender_type = message_data.get("sender_type", "OPERATOR").upper()
                message_type = message_data.get("message_type", "TEXT").upper()

                content_value = message_data.get("content", "")
                if await _is_duplicate_message(
                    chat_id, sender_type, message_type, content_value
                ):
                    try:
                        await websocket.send_json(
                            {
                                "type": "duplicate_message",
                                "chat_id": chat_id,
                                "message_type": message_type,
                                "timestamp": datetime.utcnow().isoformat(),
                            }
                        )
                    except Exception:
                        pass
                    continue

                try:
                    db_message = await crud.create_message_and_update_chat(
                        chat_id=chat_id,
                        sender_type=sender_type,
                        content=content_value,
                        message_type=message_type,
                        status="SENT",
                        source="web",
                    )
                except Exception as e:
                    await websocket.send_json(
                        {"error": "Failed to save message", "detail": str(e)}
                    )
                    continue

                file_urls = []
                if message_data.get("files"):
                    file_urls.extend(message_data.get("files") or [])
                if message_data.get("image_url"):
                    file_urls.append(message_data.get("image_url"))
                if message_data.get("file_url"):
                    file_urls.append(message_data.get("file_url"))

                normalized_files = []
                for url in file_urls:
                    if isinstance(url, str):
                        normalized_files.append(_normalize_telegram_file_url(url))
                file_urls = normalized_files

                if file_urls:
                    for url in file_urls:
                        try:
                            await database.execute(
                                pictures.insert().values(
                                    entity="messages",
                                    entity_id=db_message["id"],
                                    url=url,
                                    is_main=False,
                                    is_deleted=False,
                                    owner=user.user,
                                    cashbox=user.cashbox_id,
                                )
                            )
                        except Exception:
                            pass

                    if (
                        message_type != "TEXT"
                        and isinstance(db_message.get("content"), str)
                        and db_message.get("content", "").strip().startswith("[")
                    ):
                        try:
                            await crud.update_message(
                                db_message["id"], content=file_urls[0]
                            )
                        except Exception:
                            pass

                if sender_type == "OPERATOR":
                    try:
                        channel = await crud.get_channel(chat["channel_id"])
                        if (
                            channel
                            and channel.get("type") == "TELEGRAM"
                            and chat.get("external_chat_id")
                        ):
                            from api.chats.avito.avito_factory import (
                                _decrypt_credential,
                            )
                            from api.chats.telegram.telegram_client import (
                                send_document,
                                send_message,
                                send_photo,
                                send_video,
                            )

                            creds = await database.fetch_one(
                                channel_credentials.select().where(
                                    (channel_credentials.c.channel_id == channel["id"])
                                    & (
                                        channel_credentials.c.cashbox_id
                                        == user.cashbox_id
                                    )
                                    & (channel_credentials.c.is_active.is_(True))
                                )
                            )
                            if creds:
                                bot_token = _decrypt_credential(creds["api_key"])
                                chat_id_external = chat["external_chat_id"]
                                payload_url = file_urls[0] if file_urls else None

                                if message_type == "IMAGE" and payload_url:
                                    send_result = await send_photo(
                                        bot_token,
                                        chat_id_external,
                                        payload_url,
                                        caption=message_data.get("content"),
                                    )
                                elif message_type == "VIDEO" and payload_url:
                                    send_result = await send_video(
                                        bot_token,
                                        chat_id_external,
                                        payload_url,
                                        caption=message_data.get("content"),
                                    )
                                elif message_type == "DOCUMENT" and payload_url:
                                    send_result = await send_document(
                                        bot_token,
                                        chat_id_external,
                                        payload_url,
                                        caption=message_data.get("content"),
                                    )
                                else:
                                    send_result = await send_message(
                                        bot_token,
                                        chat_id_external,
                                        message_data.get("content", ""),
                                    )

                                if send_result and send_result.get("message_id"):
                                    await crud.update_message(
                                        db_message["id"],
                                        external_message_id=str(
                                            send_result.get("message_id")
                                        ),
                                        status="DELIVERED",
                                    )
                                else:
                                    await crud.update_message(
                                        db_message["id"], status="FAILED"
                                    )
                    except Exception as e:
                        logger.warning(
                            f"Failed to send Telegram message via websocket: {e}",
                            exc_info=True,
                        )
                        try:
                            await crud.update_message(db_message["id"], status="FAILED")
                        except Exception:
                            pass

                try:
                    await chat_producer.send_message(
                        chat_id,
                        {
                            "message_id": db_message["id"],
                            "sender_type": sender_type,
                            "content": message_data.get("content", ""),
                            "message_type": message_type,
                            "timestamp": datetime.utcnow().isoformat(),
                        },
                    )
                except Exception:
                    pass

            elif event_type == "typing":
                is_typing = message_data.get("is_typing", False)

                try:
                    await chat_producer.send_typing_event(
                        chat_id, user.user, user_type, is_typing
                    )
                except Exception as e:
                    pass

            elif event_type == "get_users":
                users = chat_manager.get_connected_users(chat_id)
                await websocket.send_json(
                    {
                        "type": "users_list",
                        "chat_id": chat_id,
                        "users": users,
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                )

            else:
                await websocket.send_json(
                    {"error": "Unknown event type", "type": event_type}
                )

    except WebSocketDisconnect:
        connection_info = await chat_manager.disconnect(chat_id, websocket)
        if connection_info:
            try:
                await chat_producer.send_user_disconnected_event(
                    chat_id, connection_info.user_id, connection_info.user_type
                )
            except Exception as e:
                pass
    except Exception as e:
        try:
            connection_info = await chat_manager.disconnect(chat_id, websocket)
            if connection_info:
                try:
                    await chat_producer.send_user_disconnected_event(
                        chat_id, connection_info.user_id, connection_info.user_type
                    )
                except Exception as e2:
                    pass
        except Exception as disconnect_error:
            pass

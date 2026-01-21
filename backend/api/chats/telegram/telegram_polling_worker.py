import asyncio
import logging
import os
from typing import Dict, Tuple

from api.chats.avito.avito_factory import _decrypt_credential
from api.chats.telegram.telegram_client import (
    TelegramAPIError,
    delete_webhook,
    get_updates,
)
from api.chats.telegram.telegram_handler import handle_update
from database.db import channel_credentials, channels, database
from sqlalchemy import and_, select

logger = logging.getLogger(__name__)

POLL_TIMEOUT = int(os.getenv("TELEGRAM_POLL_TIMEOUT", "25"))
POLL_LIMIT = int(os.getenv("TELEGRAM_POLL_LIMIT", "100"))
POLL_ERROR_SLEEP = float(os.getenv("TELEGRAM_POLL_ERROR_SLEEP", "5"))
REFRESH_INTERVAL = int(os.getenv("TELEGRAM_POLL_REFRESH_INTERVAL", "60"))

_offsets: Dict[str, int] = {}
_tasks: Dict[str, asyncio.Task] = {}
_task_meta: Dict[str, Tuple[int, int]] = {}


async def _fetch_telegram_credentials() -> Dict[str, Dict[str, int]]:
    query = (
        select(
            [
                channel_credentials.c.channel_id,
                channel_credentials.c.cashbox_id,
                channel_credentials.c.api_key,
            ]
        )
        .select_from(
            channels.join(
                channel_credentials, channels.c.id == channel_credentials.c.channel_id
            )
        )
        .where(
            and_(
                channels.c.type == "TELEGRAM",
                channels.c.is_active.is_(True),
                channel_credentials.c.is_active.is_(True),
            )
        )
    )
    rows = await database.fetch_all(query)
    credentials: Dict[str, Dict[str, int]] = {}
    for row in rows:
        encrypted_token = row["api_key"]
        if not encrypted_token:
            continue
        try:
            token = _decrypt_credential(encrypted_token)
        except Exception as exc:
            logger.warning("Failed to decrypt Telegram token: %s", exc)
            continue
        if token in credentials:
            logger.warning(
                "Duplicate Telegram token found for multiple channels: %s", token
            )
            continue
        credentials[token] = {
            "channel_id": row["channel_id"],
            "cashbox_id": row["cashbox_id"],
        }
    return credentials


async def _poll_bot_forever(token: str, channel_id: int, cashbox_id: int) -> None:
    try:
        await delete_webhook(token)
    except Exception as exc:
        logger.warning("Failed to delete webhook for token %s: %s", token, exc)

    while True:
        try:
            updates = await get_updates(
                token,
                offset=_offsets.get(token),
                timeout=POLL_TIMEOUT,
                limit=POLL_LIMIT,
            )
            for update in updates:
                update_id = update.get("update_id")
                if update_id is not None:
                    _offsets[token] = update_id + 1
                await handle_update(update, channel_id, cashbox_id, token)
        except TelegramAPIError as exc:
            logger.warning("Telegram API error for token %s: %s", token, exc)
            await asyncio.sleep(POLL_ERROR_SLEEP)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Polling error for token %s: %s", token, exc)
            await asyncio.sleep(POLL_ERROR_SLEEP)


async def _sync_tasks() -> None:
    credentials = await _fetch_telegram_credentials()
    desired_tokens = set(credentials.keys())
    active_tokens = set(_tasks.keys())

    for token in active_tokens - desired_tokens:
        task = _tasks.pop(token)
        _task_meta.pop(token, None)
        task.cancel()

    for token, data in credentials.items():
        meta = (data["channel_id"], data["cashbox_id"])
        if token in _tasks and _task_meta.get(token) == meta:
            continue
        if token in _tasks:
            _tasks[token].cancel()
        _task_meta[token] = meta
        _tasks[token] = asyncio.create_task(
            _poll_bot_forever(token, data["channel_id"], data["cashbox_id"])
        )


async def run_polling_forever(manage_db: bool = True) -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
    if manage_db:
        await database.connect()
    try:
        while True:
            await _sync_tasks()
            await asyncio.sleep(REFRESH_INTERVAL)
    finally:
        for task in _tasks.values():
            task.cancel()
        if manage_db:
            await database.disconnect()


async def main() -> None:
    await run_polling_forever(manage_db=True)


if __name__ == "__main__":
    asyncio.run(main())

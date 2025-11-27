import os
import logging
from datetime import datetime

from database.db import database, channels, organizations, channel_credentials
from api.chats.avito.avito_factory import _encrypt_credential
from api.chats.avito.avito_constants import AVITO_SVG_ICON

logger = logging.getLogger(__name__)


async def init_avito_credentials():
    try:
        api_key = os.getenv("AVITO_API_KEY")
        api_secret = os.getenv("AVITO_API_SECRET")
        access_token = os.getenv("AVITO_ACCESS_TOKEN")

        if not api_key or not api_secret:
            logger.info("AVITO_API_KEY/AVITO_API_SECRET not set — skipping Avito init")
            return

        avito_channel = await database.fetch_one(
            channels.select().where(channels.c.type == "AVITO")
        )

        if not avito_channel:
            channel_id = await database.execute(
                channels.insert().values(
                    name="Avito",
                    type="AVITO",
                    description="Avito White API Integration",
                    svg_icon=AVITO_SVG_ICON,
                    is_active=True,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
            )
            avito_channel = await database.fetch_one(channels.select().where(channels.c.id == channel_id))
        else:
            # Обновляем иконку для существующего канала, если она отсутствует
            if not avito_channel.get('svg_icon'):
                await database.execute(
                    channels.update().where(
                        channels.c.id == avito_channel["id"]
                    ).values(
                        svg_icon=AVITO_SVG_ICON,
                        updated_at=datetime.utcnow()
                    )
                )
                logger.info(f"Updated Avito channel icon for channel {avito_channel['id']}")

        channel_id = avito_channel["id"]

        org = await database.fetch_one(
            organizations.select().where(organizations.c.is_deleted.is_not(True)).limit(1)
        )

        if not org:
            logger.warning("No organization found to attach Avito credentials — skipping init")
            return

        cashbox_id = org.get("cashbox") or org.get("owner")
        if not cashbox_id:
            logger.warning("Organization has no cashbox defined — skipping Avito init")
            return

        refresh_token = None
        token_expires_at = None
        
        if not access_token:
            try:
                from api.chats.avito.avito_client import AvitoClient
                temp_client = AvitoClient(api_key, api_secret)
                token_data = await temp_client.get_access_token()
                access_token = token_data.get('access_token')
                refresh_token = token_data.get('refresh_token')
                expires_at_str = token_data.get('expires_at')
                if expires_at_str:
                    from datetime import datetime as dt
                    token_expires_at = dt.fromisoformat(expires_at_str)
                logger.info("Successfully obtained initial access_token from Avito API")
            except Exception as e:
                logger.error(f"Failed to obtain access_token from Avito API: {e}")
        
        encrypted_api_key = _encrypt_credential(api_key)
        encrypted_api_secret = _encrypt_credential(api_secret)
        encrypted_access_token = _encrypt_credential(access_token) if access_token else None
        encrypted_refresh_token = _encrypt_credential(refresh_token) if refresh_token else None

        existing = await database.fetch_one(
            channel_credentials.select().where(
                (channel_credentials.c.channel_id == channel_id) &
                (channel_credentials.c.cashbox_id == cashbox_id)
            )
        )

        if existing:
            update_values = {
                "api_key": encrypted_api_key,
                "api_secret": encrypted_api_secret,
                "access_token": encrypted_access_token,
                "is_active": True,
                "updated_at": datetime.utcnow(),
            }
            if encrypted_refresh_token:
                update_values["refresh_token"] = encrypted_refresh_token
            if token_expires_at:
                update_values["token_expires_at"] = token_expires_at
            
            await database.execute(
                channel_credentials.update().where(
                    channel_credentials.c.id == existing["id"]
                ).values(**update_values)
            )
            logger.info(f"Updated Avito credentials for channel={channel_id} cashbox={cashbox_id}")
        else:
            insert_values = {
                "channel_id": channel_id,
                "cashbox_id": cashbox_id,
                "api_key": encrypted_api_key,
                "api_secret": encrypted_api_secret,
                "access_token": encrypted_access_token,
                "is_active": True,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }
            if encrypted_refresh_token:
                insert_values["refresh_token"] = encrypted_refresh_token
            if token_expires_at:
                insert_values["token_expires_at"] = token_expires_at
            
            await database.execute(
                channel_credentials.insert().values(**insert_values)
            )
            logger.info(f"Inserted Avito credentials for channel={channel_id} cashbox={cashbox_id}")

    except Exception as e:
        logger.error(f"Failed to init Avito credentials from env: {e}", exc_info=True)

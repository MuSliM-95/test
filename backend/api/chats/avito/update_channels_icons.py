"""
Скрипт для обновления иконок у всех существующих каналов Avito
"""
import asyncio
import logging
from datetime import datetime

from database.db import database, channels
from api.chats.avito.avito_constants import AVITO_SVG_ICON

logger = logging.getLogger(__name__)


async def update_avito_channels_icons():
    """
    Обновляет иконки для всех каналов Avito, у которых отсутствует иконка
    """
    try:
        await database.connect()
        
        # Находим все каналы Avito без иконки
        avito_channels = await database.fetch_all(
            channels.select().where(
                (channels.c.type == 'AVITO') &
                ((channels.c.svg_icon.is_(None)) | (channels.c.svg_icon == ''))
            )
        )
        
        if not avito_channels:
            logger.info("No Avito channels without icons found")
            return
        
        logger.info(f"Found {len(avito_channels)} Avito channels without icons. Updating...")
        
        updated_count = 0
        for channel in avito_channels:
            try:
                await database.execute(
                    channels.update().where(
                        channels.c.id == channel['id']
                    ).values(
                        svg_icon=AVITO_SVG_ICON,
                        updated_at=datetime.utcnow()
                    )
                )
                updated_count += 1
                logger.info(f"Updated icon for Avito channel {channel['id']} ({channel['name']})")
            except Exception as e:
                logger.error(f"Failed to update icon for channel {channel['id']}: {e}")
        
        logger.info(f"Successfully updated icons for {updated_count} Avito channels")
        
    except Exception as e:
        logger.error(f"Error updating Avito channel icons: {e}", exc_info=True)
    finally:
        await database.disconnect()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(update_avito_channels_icons())








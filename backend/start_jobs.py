import asyncio
import logging

from database.db import database  # Импорт базы данных
from jobs.jobs import scheduler  # Импорт настроенного планировщика

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    logger.info("Starting Job Scheduler...")

    # 1. Глобальное подключение к БД при старте
    if not database.is_connected:
        await database.connect()
        logger.info("Database connected globally.")
    scheduler.start()

    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopping scheduler...")
        scheduler.shutdown()
    finally:
        if database.is_connected:
            await database.disconnect()
            logger.info("Database disconnected.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass

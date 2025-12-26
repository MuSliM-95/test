import functools
import logging

from asyncpg.exceptions import InterfaceError

from database.db import database

logger = logging.getLogger(__name__)


def ensure_db_connection(func):
    """
    Декоратор для фоновых задач (Jobs) и скриптов.

    Обеспечивает:
    1. Подключение к БД, если оно еще не установлено (для работы в отдельном контейнере jobs).
    2. Использование существующего соединения, если оно есть (для работы внутри API).
    3. Корректную обработку остановки приложения (InterfaceError: pool is closing).
    4. Отключение от БД только в том случае, если соединение было открыто этим декоратором.
    """

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        should_disconnect = False

        try:
            # Если база не подключена - подключаемся и запоминаем, что мы открыли соединение
            if not database.is_connected:
                await database.connect()
                should_disconnect = True

            return await func(*args, **kwargs)

        except InterfaceError as e:
            # Если пул закрывается (приложение останавливается), просто логируем и выходим
            if "pool is closing" in str(e):
                logger.info(
                    f"Task '{func.__name__}' interrupted/skipped: Database pool is closing."
                )
                return
            # Если другая ошибка интерфейса БД - логируем как ошибку
            logger.error(
                f"Database interface error in '{func.__name__}': {e}", exc_info=True
            )
            raise e

        except Exception as e:
            # Логируем любые другие ошибки, чтобы джоба не падала молча
            logger.error(
                f"Critical error in task '{func.__name__}': {e}", exc_info=True
            )
            raise e

        finally:
            # Отключаемся, только если мы сами инициировали подключение
            if should_disconnect and database.is_connected:
                await database.disconnect()

    return wrapper

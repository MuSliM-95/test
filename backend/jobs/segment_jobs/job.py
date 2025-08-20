import time

from database.db import database, segments
from segments.main import update_segment_task

from sqlalchemy import select, cast, func, Integer, and_, or_
from sqlalchemy.dialects.postgresql import JSONB

from segments.logger import logger


async def get_segment_ids():
    # Приведение update_settings к JSONB
    jsonb_field = cast(segments.c.update_settings, JSONB)

    # Извлечение interval_minutes и приведение к Integer
    interval_minutes = cast(
        func.jsonb_extract_path_text(jsonb_field, 'interval_minutes'),
        Integer
    )

    # Строим основной запрос
    query = select(segments.c.id).where(
        and_(
            segments.c.type_of_update == 'cron',
            segments.c.is_archived.isnot(True),
            segments.c.is_deleted.isnot(True),
            segments.c.update_settings['interval_minutes'].isnot(None),
            or_(segments.c.updated_at.is_(None),
                segments.c.updated_at <= func.now() - func.make_interval(
                    0, 0, 0, 0, 0, interval_minutes)
                )
        )
    )
    rows = await database.fetch_all(query)
    return [row.id for row in rows]


async def segment_update():
    segment_ids = await get_segment_ids()
    start = time.time()
    for segment_id in segment_ids:
        await update_segment_task(segment_id)
    logger.info(f'Segments updated in {time.time() - start:.2f} seconds. Total segments: {len(segment_ids)}')

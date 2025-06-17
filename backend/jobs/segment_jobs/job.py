from datetime import datetime, timedelta

from database.db import database, segments
from functions.segments import update_segment_task

from sqlalchemy import select, cast, func, Integer, and_
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql.psycopg2 import dialect


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
            segments.c.update_settings['interval_minutes'].isnot(None),
            segments.c.updated_at <= func.now() - func.make_interval(
                0, 0, 0, 0, 0, interval_minutes)
        )
    )
    rows = await database.fetch_all(query)
    return [row.id for row in rows]


async def segment_update():
    segment_ids = await get_segment_ids()
    for segment_id in segment_ids:
        await update_segment_task(segment_id)





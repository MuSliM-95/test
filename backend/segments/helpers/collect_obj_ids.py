from sqlalchemy import select

from database.db import SegmentObjectType, SegmentChangeType, \
    segment_version_objects, database, segment_versions


async def collect_objects(segment_id, version, obj_type: SegmentObjectType, change_type: SegmentChangeType):
    """Получение списка id объектов в версии среза."""

    query = (
        select(segment_version_objects.c.object_id)
        .outerjoin(segment_versions, segment_version_objects.c.version == segment_versions.c.id)
        .where(
            segment_version_objects.c.segment_id == segment_id,
            segment_versions.c.version == version,
            segment_version_objects.c.object_type == obj_type,
            segment_version_objects.c.change_type == change_type
        )
    )
    rows = await database.fetch_all(query)
    obj_ids = [row["object_id"] for row in rows]
    return obj_ids

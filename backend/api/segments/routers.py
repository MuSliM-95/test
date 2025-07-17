import asyncio
import json
from typing import List

from fastapi import APIRouter, HTTPException

from api.segments import schemas
from database.db import segments, database, client_segments, SegmentStatus
from functions.helpers import get_user_by_token
from segments.main import Segments, update_segment_task

router = APIRouter(tags=["segments"])


@router.post("/segments", response_model=schemas.Segment)
async def create_segments(token: str, segment_data: schemas.SegmentCreate):
    user = await get_user_by_token(token)

    data = segment_data.dict(exclude_none=True)

    query = segments.insert().values(
        name=segment_data.name,
        criteria=data.get("criteria"),
        actions=data.get("actions"),
        cashbox_id=user.cashbox_id,
        type_of_update=data.get("type_of_update"),
        update_settings=data.get("update_settings"),
        status=SegmentStatus.in_process.value,
        is_archived=data.get("is_archived"),
        selection_field=data.get("selection_field"),
    )

    new_segment_id = await database.execute(query)

    asyncio.create_task(update_segment_task(new_segment_id))
    segment = await database.fetch_one(
        segments.select()
        .where(segments.c.id == new_segment_id)
    )
    return schemas.Segment(
        id=segment.id,
        name=segment.name,
        criteria=json.loads(segment.criteria),
        actions=json.loads(segment.actions) if segment.actions else {},
        updated_at=segment.updated_at,
        type_of_update=segment.type_of_update,
        update_settings=json.loads(segment.update_settings),
        status=segment.status,
        is_archived=segment.is_archived,
        selection_field=segment.selection_field,
    )

@router.post("/segments/{idx}", response_model=schemas.Segment)
async def refresh_segments(idx: int, token: str):
    user = await get_user_by_token(token)
    query = segments.select().where(segments.c.id == idx, segments.c.cashbox_id == user.cashbox_id)
    segment = await database.fetch_one(query)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")
    if segment.is_archived:
        raise HTTPException(status_code=403, detail="Сегмент заархивирован!")
    if segment.updated_at and datetime.now(timezone.utc) - segment.updated_at < timedelta(minutes=5):
        raise HTTPException(status_code=403, detail="Сегмент обновлен менее 5 минут назад!")

    await database.execute(
        segments.update().where(segments.c.id == segment.id)
        .values(status=SegmentStatus.in_process.value)
    )
    asyncio.create_task(update_segment_task(segment.id))
    segment = await database.fetch_one(
        segments.select()
        .where(segments.c.id == idx)
    )
    return schemas.Segment(
        id=segment.id,
        name=segment.name,
        criteria=json.loads(segment.criteria),
        actions=json.loads(segment.actions) if segment.actions else {},
        updated_at=segment.updated_at,
        type_of_update=segment.type_of_update,
        update_settings=json.loads(segment.update_settings),
        status=segment.status,
        is_archived=segment.is_archived,
        selection_field=segment.selection_field,
    )


@router.get("/segments/{idx}", response_model=schemas.Segment)
async def get_segment(idx: int, token: str):
    user = await get_user_by_token(token)
    query = segments.select().where(segments.c.id == idx,
                                    segments.c.cashbox_id == user.cashbox_id)
    segment = await database.fetch_one(query)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")
    if segment.is_archived:
        raise HTTPException(status_code=403, detail="Сегмент заархивирован!")
    return schemas.Segment(
        id=segment.id,
        name=segment.name,
        criteria=json.loads(segment.criteria),
        actions=json.loads(segment.actions) if segment.actions else {},
        updated_at=segment.updated_at,
        type_of_update=segment.type_of_update,
        update_settings=json.loads(segment.update_settings),
        status=segment.status,
        is_archived=segment.is_archived,
        selection_field=segment.selection_field,
    )


@router.put("/segments/{idx}", response_model=schemas.Segment)
async def update_segments(idx: int, token: str, segment_data: schemas.SegmentCreate):
    user = await get_user_by_token(token)
    query = segments.select().where(segments.c.id == idx,
                                    segments.c.cashbox_id == user.cashbox_id)
    segment = await database.fetch_one(query)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")

    data = segment_data.dict(exclude_none=True)

    query = segments.update().where(segments.c.id == idx).values(
        name=segment_data.name,
        criteria=data.get("criteria"),
        actions=data.get("actions"),
        cashbox_id=user.cashbox_id,
        type_of_update=data.get("type_of_update"),
        update_settings=data.get("update_settings"),
        status=SegmentStatus.in_process.value,
        is_archived=data.get("is_archived"),
        selection_field=data.get("selection_field"),
    )

    await database.execute(query)

    asyncio.create_task(update_segment_task(idx))
    segment = await database.fetch_one(
        segments.select()
        .where(segments.c.id == idx)
    )
    return schemas.Segment(
        id=segment.id,
        name=segment.name,
        criteria=json.loads(segment.criteria),
        actions=json.loads(segment.actions),
        updated_at=segment.updated_at,
        type_of_update=segment.type_of_update,
        update_settings=json.loads(segment.update_settings),
        status=segment.status,
        is_archived=segment.is_archived,
        selection_field=segment.selection_field,
    )


@router.get("/segments/{idx}/result", response_model=schemas.SegmentData)
async def get_segment_data(idx: int, token: str):
    user = await get_user_by_token(token)
    segment = Segments(idx)
    await segment.async_init()
    if not segment.segment_obj or segment.segment_obj.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=404, detail="Сегмент не найден")
    contragents_data = await segment.collect_data()
    if not contragents_data:
        return {}

    return schemas.SegmentData(
        id=segment.segment_id,
        updated_at=segment.segment_obj.updated_at,
        **contragents_data,
    )


@router.get("/segments", response_model=List[schemas.Segment])
async def get_user_segments(token: str):
    user = await get_user_by_token(token)

    query = segments.select().where(segments.c.cashbox_id == user.cashbox_id)

    rows = await database.fetch_all(query)
    return [schemas.Segment(
        id=row.id,
        name=row.name,
        criteria=json.loads(row.criteria),
        actions=json.loads(row.actions) if row.actions else {},
        updated_at=row.updated_at,
        type_of_update=row.type_of_update,
        update_settings=json.loads(row.update_settings),
        status=row.status,
        is_archived=row.is_archived,
        selection_field=row.selection_field,
    ) for row in rows]

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Response

from api.segments import schemas
from database.db import segments, database, SegmentStatus
from functions.helpers import get_user_by_token, sanitize_float, deep_sanitize
from segments.main import Segments, update_segment_task
from sqlalchemy import func

router = APIRouter(tags=["segments"])


@router.post("/segments/", response_model=schemas.Segment)
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
    if segment.is_deleted:
        raise HTTPException(status_code=403, detail="Сегмент удален!")
    # if segment.updated_at and datetime.now(timezone.utc) - segment.updated_at < timedelta(minutes=5):
    #     raise HTTPException(status_code=403, detail="Сегмент обновлен менее 5 минут назад!")

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
    if segment.is_deleted:
        raise HTTPException(status_code=403, detail="Сегмент удален!")
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
    if segment.is_deleted:
        raise HTTPException(status_code=403, detail="Сегмент удален!")

    data = segment_data.dict(exclude_none=True)

    query = segments.update().where(segments.c.id == idx).values(
        name=segment_data.name,
        criteria=data.get("criteria"),
        actions=data.get("actions"),
        cashbox_id=user.cashbox_id,
        type_of_update=data.get("type_of_update"),
        update_settings=data.get("update_settings"),
        status=SegmentStatus.in_process.value,
        is_archived=data.get("is_archived")
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


@router.delete("/segments/{idx}")
async def delete_segments(idx: int, token: str):
    user = await get_user_by_token(token)
    query = segments.select().where(segments.c.id == idx,
                                    segments.c.cashbox_id == user.cashbox_id)
    segment = await database.fetch_one(query)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")

    query = segments.update().where(segments.c.id == idx).values(
        is_deleted = True,
        updated_at=func.now(),
    )

    await database.execute(query)

    return Response(status_code=204)


@router.get("/segments/{idx}/result", response_model=schemas.SegmentData)
async def get_segment_data(idx: int, token: str):
    user = await get_user_by_token(token)
    segment = Segments(idx)
    await segment.async_init()
    if not segment.segment_obj or segment.segment_obj.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=404, detail="Сегмент не найден")
    if segment.segment_obj.is_deleted:
        raise HTTPException(status_code=403, detail="Сегмент удален!")
    contragents_data = await segment.collect_data()

    return schemas.SegmentData(
        id=segment.segment_id,
        updated_at=segment.segment_obj.updated_at,
        **contragents_data,
    )


@router.get("/segments/", response_model=List[schemas.SegmentWithContragents])
async def get_user_segments(token: str, is_archived: Optional[bool] = None):
    user = await get_user_by_token(token)

    query = segments.select().where(segments.c.cashbox_id == user.cashbox_id, segments.c.is_deleted.isnot(True))

    if is_archived is not None:
        query = query.where(segments.c.is_archived == is_archived)

    rows = await database.fetch_all(query)

    result = []

    for row in rows:
        segment = Segments(row.id)
        await segment.async_init()
        if not segment.segment_obj or segment.segment_obj.cashbox_id != user.cashbox_id:
            raise HTTPException(status_code=404, detail="Сегмент не найден")
        contragents_data = await segment.collect_data()

        
        sanitized_criteria = json.loads(row.criteria)
        sanitized_actions = json.loads(row.actions) if row.actions else {}
        sanitized_update_settings = json.loads(row.update_settings)

        sanitized_criteria = deep_sanitize(sanitized_criteria)
        sanitized_actions = deep_sanitize(sanitized_actions)
        sanitized_update_settings = deep_sanitize(sanitized_update_settings)

        result.append(schemas.SegmentWithContragents(
            id=row.id,
            name=row.name,
            criteria=sanitized_criteria,
            actions=sanitized_actions,
            updated_at=row.updated_at,
            type_of_update=row.type_of_update,
            update_settings=sanitized_update_settings,
            status=row.status,
            is_archived=row.is_archived,
            selection_field=row.selection_field,
            contragents_count=len(contragents_data["contragents"]),
            added_contragents_count=len(contragents_data["added_contragents"]),
            deleted_contragents_count=len(contragents_data["deleted_contragents"]),
            entered_contragents_count=len(contragents_data["entered_contragents"]),
            exited_contragents_count=len(contragents_data["exited_contragents"]),
        ))  


    return result

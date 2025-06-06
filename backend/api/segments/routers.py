import asyncio
import json
import uuid
from asyncio import sleep
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Request

from api.segments import schemas
from database.db import segments, database, client_segments
from functions.helpers import get_user_by_token
from functions.segments import Segments, update_segment_task, SegmentStatus
from sqlalchemy import select, func

router = APIRouter(tags=["segments"])


@router.post("/segments", response_model=schemas.Segment)
async def create_segments(token: str, segment_data: schemas.SegmentCreate, request: Request):
    user = await get_user_by_token(token)

    data = segment_data.dict(exclude_none=True)

    new_uuid = uuid.uuid4()
    query = segments.select().where(segments.c.uuid == new_uuid)

    while await database.execute(query):
        new_uuid = uuid.uuid4()

    query = segments.insert().values(
        name=segment_data.name,
        criteria=data.get("criteria"),
        created_by=user.id,
        uuid=new_uuid,
        type_of_update=data.get("type_of_update"),
        update_settings=data.get("update_settings"),
        status=SegmentStatus.in_process.value,
    )

    new_segment_id = await database.execute(query)

    if data.get("type_of_update") == "cron":
        # добавить в крон

        pass

    asyncio.create_task(update_segment_task(new_segment_id))
    segment = await database.fetch_one(
        segments.select()
        .outerjoin(client_segments,
                   client_segments.c.segment_id == segments.c.id)
        .where(segments.c.id == new_segment_id)
    )
    root_path = request.scope.get("root_path", "")
    return schemas.Segment(
        id=segment.id,
        name=segment.name,
        criteria=json.loads(segment.criteria),
        updated_at=segment.updated_at,
        type_of_update=segment.type_of_update,
        update_settings=json.loads(segment.update_settings),
        status=segment.status,
        update_url=f"{root_path}/segments/{segment.uuid}",
        result_url=f"{root_path}/segments/{segment.uuid}/result",
    )

@router.post("/segments/{uuid}", response_model=schemas.Segment)
async def update_segments(uuid: uuid.UUID, token: str, request: Request):
    user = await get_user_by_token(token)
    query = segments.select().where(segments.c.uuid == uuid, segments.c.created_by == user.id)
    segment = await database.fetch_one(query)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")
    # if datetime.now(timezone.utc) - segment.updated_at < timedelta(minutes=5):
    #     raise HTTPException(status_code=403, detail="Сегмент обновлен менее 5 минут назад!")
    asyncio.create_task(update_segment_task(segment.id))
    await database.execute(
        segments.update().where(segments.c.id == segment.id)
        .values(status=SegmentStatus.in_process.value)
    )
    segment = await database.fetch_one(
        segments.select()
        .outerjoin(client_segments,
                   client_segments.c.segment_id == segments.c.id)
        .where(segments.c.uuid == uuid, segments.c.created_by == user.id)
    )
    root_path = request.scope.get("root_path", "")
    return schemas.Segment(
        id=segment.id,
        name=segment.name,
        criteria=json.loads(segment.criteria),
        updated_at=segment.updated_at,
        type_of_update=segment.type_of_update,
        update_settings=json.loads(segment.update_settings),
        status=segment.status,
        update_url=f"{root_path}/segments/{segment.uuid}",
        result_url=f"{root_path}/segments/{segment.uuid}/result",
    )


@router.get("/segments/{uuid}/result", response_model=schemas.SegmentData)
async def get_segment_data(uuid: uuid.UUID, token: str):
    user = await get_user_by_token(token)
    segment = Segments()
    await segment.obj_by_uuid(uuid)
    if not segment.segment_obj or segment.segment_obj.created_by != user.id:
        raise HTTPException(status_code=404, detail="Сегмент не найден")

    contragents_data = await segment.collect_data()
    return schemas.SegmentData(
        id=segment.segment_obj.id,
        updated_at=segment.segment_obj.updated_at,
        **contragents_data,
    )

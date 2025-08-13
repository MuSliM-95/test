import json
from datetime import datetime

from database.db import segments, database, SegmentStatus, users_cboxes_relation

from segments.logic.logic import SegmentLogic
from segments.query.queries import SegmentCriteriaQuery
from segments.actions.actions import SegmentActions

from segments.logic.collect_data import ContragentsData

from segments.logger import logger
from segments.websockets import notify
from segments.query.queries import get_token_by_segment_id
import asyncio


class Segments:
    def __init__(self, segment_id: int = None):
        self.segment_id = segment_id
        self.segment_obj = None
        self.logic = None
        self.query = None
        self.actions = None

    async def async_init(self):
        self.segment_obj = await database.fetch_one(
            segments.select().where(segments.c.id == self.segment_id))
        self.logic = SegmentLogic(self.segment_obj)
        self.query = SegmentCriteriaQuery(
            self.segment_obj.cashbox_id,
            json.loads(self.segment_obj.criteria)
        )
        self.actions = SegmentActions(self.segment_obj)

    async def refresh_segment_obj(self):
        self.segment_obj = await database.fetch_one(
            segments.select().where(segments.c.id == self.segment_obj.id))

    async def update_segment_datetime(self):
        await database.execute(
            segments.update().where(segments.c.id == self.segment_id)
            .values(
                updated_at=datetime.now(),
                previous_update_at=self.segment_obj.updated_at,
            )
        )
        await self.async_init()

    async def set_status_in_progress(self):
        await database.execute(
            segments.update().where(segments.c.id == self.segment_id)
            .values(status=SegmentStatus.in_process.value)
        )

    async def set_status_calculated(self):
        await database.execute(
            segments.update().where(segments.c.id == self.segment_id)
            .values(status=SegmentStatus.calculated.value)
        )

    async def update_segment(self):
        try:
            start = datetime.now()
            await self.set_status_in_progress()
            new_ids = await self.query.collect_ids()
            await self.logic.update_segment_data_in_db(new_ids)
            await self.actions.start_actions()
            await self.update_segment_datetime()
            await self.set_status_calculated()
            logger.info(f'Segment {self.segment_id} updated successfully. Start - {start}. Took {datetime.now() - start}')
        except Exception as e:
            logger.exception(f"Ошибка при обновлении сегмента {self.segment_obj.id}: {e}")

    async def collect_data(self):
        data_obj = ContragentsData(self.segment_obj)

        return await data_obj.collect()


async def update_segment_task(segment_id: int):
    token = await get_token_by_segment_id(segment_id)
    await notify(ws_token=token, event="recalc_start", segment_id=segment_id)
    logger.info(f"Starting update for segment {segment_id} with token {token}")
    segment = Segments(segment_id)

    await segment.async_init()

    if getattr(segment.segment_obj, "is_deleted", False):
        logger.info(f"Segment {segment_id} is deleted; skip update.")
        await notify(ws_token=token, event="recalc_fail_410", segment_id=segment_id)
        return
    

    if segment.segment_obj:
        await segment.update_segment()
        await notify(ws_token=token, event="recalc_finish", segment_id=segment_id)
    else:
        await notify(ws_token=token, event="recalc_fail_404", segment_id=segment_id)



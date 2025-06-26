from datetime import datetime

from database.db import segments, database, SegmentStatus

from segments.contragents_logic import ContragentsLogic


class Segments:
    def __init__(self, segment_id: int = None):
        self.segment_id = segment_id
        self.segment_obj = None

    async def async_init(self):
        self.segment_obj = await database.fetch_one(
            segments.select().where(segments.c.id == self.segment_id))

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
        await self.set_status_in_progress()
        if self.segment_obj.selection_field is None:
            pass
        elif self.segment_obj.selection_field == 'contragents':
            logic = ContragentsLogic(self.segment_obj)
            await logic.update_contragent_segment()
        await self.update_segment_datetime()
        await self.set_status_calculated()



    async def collect_data(self):
        if self.segment_obj.selection_field is None:
            return None
        elif self.segment_obj.selection_field == 'contragents':
            logic = ContragentsLogic(self.segment_obj)
            return await logic.collect_data()




async def update_segment_task(segment_id: int):
    segment = Segments(segment_id)
    await segment.async_init()
    if segment.segment_obj:
        await segment.update_segment()

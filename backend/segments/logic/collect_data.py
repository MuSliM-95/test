from database.db import (
    contragents, database, SegmentObjectType, SegmentChangeType
)

from segments.helpers.collect_obj_ids import collect_objects


class ContragentsData:
    def __init__(self, segment_obj):
        self.segment_obj = segment_obj

    async def collect(self):
        contragents = await self.get_contragents_data()
        added_contragents = await self.added_contragents_data()
        deleted_contragents = await self.deleted_contragents_data()
        return {
            "contragents": contragents,
            "added_contragents": added_contragents,
            "deleted_contragents": deleted_contragents
        }

    async def get_contragents_data(self):
        contragents_ids = await collect_objects(self.segment_obj.id, self.segment_obj.current_version, SegmentObjectType.contragents.value, SegmentChangeType.existing.value)
        query = (
            contragents.select()
            .where(contragents.c.id.in_(contragents_ids))
        )
        objs = await database.fetch_all(query)
        return [{
            "id": obj.id,
            "name": obj.name,
            "phone": obj.phone
        } for obj in objs]

    async def added_contragents_data(self):
        contragents_ids = await collect_objects(self.segment_obj.id,
                                                self.segment_obj.current_version,
                                                SegmentObjectType.contragents.value,
                                                SegmentChangeType.added.value)
        query = (
            contragents.select()
            .where(contragents.c.id.in_(contragents_ids))
        )
        objs = await database.fetch_all(query)
        return [{
            "id": obj.id,
            "name": obj.name,
            "phone": obj.phone
        } for obj in objs]

    async def deleted_contragents_data(self):
        contragents_ids = await collect_objects(self.segment_obj.id,
                                                self.segment_obj.current_version,
                                                SegmentObjectType.contragents.value,
                                                SegmentChangeType.removed.value)
        query = (
            contragents.select()
            .where(contragents.c.id.in_(contragents_ids))
        )
        objs = await database.fetch_all(query)
        return [{
            "id": obj.id,
            "name": obj.name,
            "phone": obj.phone
        } for obj in objs]

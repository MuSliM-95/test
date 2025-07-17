from database.db import contragents, client_segments, database, client_segment_history, SegmentStatusHistory


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
        query = (
            contragents.select()
            .join(client_segments,
                  client_segments.c.contragent_id == contragents.c.id)
            .where(client_segments.c.segment_id == self.segment_obj.id)
        )
        objs = await database.fetch_all(query)
        return [{
            "id": obj.id,
            "name": obj.name,
            "phone": obj.phone
        } for obj in objs]

    async def added_contragents_data(self):
        query = (
            contragents.select()
            .join(client_segment_history,
                  client_segment_history.c.contragent_id == contragents.c.id)
            .where(client_segment_history.c.segment_id == self.segment_obj.id,
                   client_segment_history.c.status == SegmentStatusHistory.added.value)
        )
        objs = await database.fetch_all(query)
        return [{
            "id": obj.id,
            "name": obj.name,
            "phone": obj.phone
        } for obj in objs]

    async def deleted_contragents_data(self):
        query = (
            contragents.select()
            .join(client_segment_history,
                  client_segment_history.c.contragent_id == contragents.c.id)
            .where(client_segment_history.c.segment_id == self.segment_obj.id,
                   client_segment_history.c.status == SegmentStatusHistory.deleted.value)
        )
        objs = await database.fetch_all(query)
        return [{
            "id": obj.id,
            "name": obj.name,
            "phone": obj.phone
        } for obj in objs]

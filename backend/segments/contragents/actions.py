from typing import List

from segments.base.base_action import BaseAction
from sqlalchemy import select

from database.db import tags, database, contragents_tags
from sqlalchemy.dialects.postgresql import insert


class ContragentsAction(BaseAction):
    def __init__(self, segment_obj):
        super().__init__(segment_obj)
        self.ACTIONS = {
            "add_tags": self.add_tags,
            "remove_tags": self.remove_tags,
        }

    async def run(self, action, *args, **kwargs):
        if action in self.ACTIONS:
            await self.ACTIONS[action](*args, **kwargs)

    @staticmethod
    async def add_tags(contragents_ids: List[int], data: dict):
        tag_names = data.get("name", [])
        query = select(tags.c.id).where(tags.c.name.in_(tag_names))
        rows = await database.fetch_all(query)
        tag_ids = [row.id for row in rows]
        new_values = []
        for tag_id in tag_ids:
            for contragent_id in contragents_ids:
                new_values.append({"contragent_id": contragent_id, "tag_id": tag_id})

        query = insert(contragents_tags).values(new_values)
        query = query.on_conflict_do_nothing(
            index_elements=["tag_id", "contragent_id"]  # <- уникальная пара
        )
        await database.execute(query)

    @staticmethod
    async def remove_tags(contragents_ids: List[int], data: dict):
        tag_names = data.get("name", [])
        query = select(tags.c.id).where(tags.c.name.in_(tag_names))
        rows = await database.fetch_all(query)
        tag_ids = [row.id for row in rows]
        query = (
            contragents_tags.delete()
            .where(contragents_tags.c.tag_id.in_(tag_ids))
        )
        await database.execute(query)

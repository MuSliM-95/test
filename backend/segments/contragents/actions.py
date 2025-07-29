from typing import List

from segments.base.base_action import BaseAction
from sqlalchemy import select, func, and_

from database.db import tags, database, contragents_tags
from sqlalchemy.dialects.postgresql import insert


class ContragentsAction(BaseAction):
    def __init__(self, segment_obj):
        super().__init__(segment_obj)
        self.ACTIONS = {
            "add_existed_tags": self.add_existed_tags,
            "remove_tags": self.remove_tags,
            "client_tags": self.client_tags,
        }

    async def run(self, action, *args, **kwargs):
        if action in self.ACTIONS:
            await self.ACTIONS[action](*args, **kwargs)

    async def add_existed_tags(self, contragents_ids: List[int], data: dict):
        tag_names = data.get("name", [])
        query = select(tags.c.id).where(and_(tags.c.name.in_(tag_names), tags.c.cashbox_id == self.segment_obj.cashbox_id))
        rows = await database.fetch_all(query)
        tag_ids = [row.id for row in rows]
        new_values = []
        for tag_id in tag_ids:
            for contragent_id in contragents_ids:
                new_values.append({"contragent_id": contragent_id, "tag_id": tag_id, "cashbox_id": self.segment_obj.cashbox_id})
        if not new_values:
            return
        query = insert(contragents_tags).values(new_values)
        query = query.on_conflict_do_nothing(
            index_elements=["tag_id", "contragent_id"]  # <- уникальная пара
        )
        await database.execute(query)

    async def remove_tags(self, contragents_ids: List[int], data: dict):
        tag_names = data.get("name", [])
        query = select(tags.c.id).where(tags.c.name.in_(tag_names), tags.c.cashbox_id == self.segment_obj.cashbox_id)
        rows = await database.fetch_all(query)
        tag_ids = [row.id for row in rows]
        query = (
            contragents_tags.delete()
            .where(contragents_tags.c.tag_id.in_(tag_ids), contragents_tags.c.contragent_id.in_(contragents_ids))
        )
        await database.execute(query)

    async def client_tags(self, contragents_ids: List[int], data: dict):
        names = []
        prepared_data = []
        tags_data = data.get("tags", [])
        for d in tags_data:
            names.append(d["name"])
            prepared_data.append({
                "name": d["name"],
                "emoji": d.get("emoji", None),
                "color": d.get("color", None),
                "description": d.get("description", None),
                "cashbox_id": self.segment_obj.cashbox_id,
            })

        count_query = (
            select(func.count())
            .select_from(tags)
            .where(tags.c.name.in_(names), tags.c.cashbox_id == self.segment_obj.cashbox_id)
        )

        count_rows = await database.execute(count_query)

        if count_rows != len(set(names)):
            insert_query = insert(tags).values(prepared_data).on_conflict_do_nothing(index_elements=['name', "cashbox_id"])
            await database.execute(insert_query)

        await self.add_existed_tags(contragents_ids, {"name": names})

import json
from typing import List

from database.db import (
    docs_sales, OrderStatus, database, segments, users, users_cboxes_relation,
    docs_sales_tags, client_segments, client_segment_history,
    SegmentStatusHistory
)

from segments.base.base_logic import BaseSegmentLogic
from segments.contragents.criteria_query import ContragentsCriteriaQuery
from sqlalchemy import select

from segments.contragents.collect_data import ContragentsData


class ContragentsLogic(BaseSegmentLogic):

    async def update_segment(self):

        criteria_data = json.loads(self.segment_obj.criteria)
        criteria_query_obj = ContragentsCriteriaQuery(self.segment_obj.cashbox_id, criteria_data)
        query = criteria_query_obj.get_query()

        rows = await database.fetch_all(query)
        contragent_to_segment = [row.id for row in rows]
        await self.update_id_in_segment(contragent_to_segment)
        contragent_in_segment = await self.get_contragent_in_segment()
        ids_to_segment = list(
            set(contragent_to_segment) - set(contragent_in_segment))
        ids_out_of_segment = list(
            set(contragent_in_segment) - set(contragent_to_segment))
        if ids_to_segment:
            await self.add_contragents_to_segment(ids_to_segment)
        if ids_out_of_segment:
            await self.remove_contragents_from_segment(ids_out_of_segment)

        return contragent_to_segment

    async def get_contragent_in_segment(self):
        query = select(client_segments.c.contragent_id).where(
            client_segments.c.segment_id == self.segment_obj.id)
        contragent_ids = await database.fetch_all(query)
        return [row.contragent_id for row in contragent_ids]

    async def add_contragents_to_segment(self, contragent_ids: list) -> None:
        query = client_segments.insert()
        history_query = client_segment_history.insert()

        values = []
        history_values = []
        for contragent_id in contragent_ids:
            values.append({"segment_id": self.segment_obj.id,
                           "contragent_id": contragent_id})
            history_values.append({
                "segment_id": self.segment_obj.id,
                "contragent_id": contragent_id,
                "status": SegmentStatusHistory.added.value
            })

        await database.execute_many(query=query, values=values)
        await database.execute_many(query=history_query, values=history_values)

    async def remove_contragents_from_segment(self,
                                              contragent_ids: list) -> None:
        query = client_segments.delete().where(
            client_segments.c.segment_id == self.segment_obj.id,
            client_segments.c.contragent_id.in_(contragent_ids),
        )
        await database.execute(query)

        history_query = client_segment_history.insert()
        history_values = []
        for contragent_id in contragent_ids:
            history_values.append({
                "segment_id": self.segment_obj.id,
                "contragent_id": contragent_id,
                "status": SegmentStatusHistory.deleted.value
            })
        await database.execute_many(query=history_query, values=history_values)

    async def collect_data(self):
        data = ContragentsData(self.segment_obj)
        return await data.collect()

    async def run_action(self, action: str, ids: List[int], data: dict = None):
        """Метод для выполения action"""
        return





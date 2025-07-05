import json
from typing import List

from database.db import docs_sales, OrderStatus, database, segments, users, users_cboxes_relation, docs_sales_tags

from segments.base_logic import BaseSegmentLogic
from segments.docs_sales.criteria_query import DocsSalesCriteriaQuery
from segments.docs_sales.actions import DocsSalesAction


class DocsSalesLogic(BaseSegmentLogic):

    async def update_segment(self):

        criteria_data = json.loads(self.segment_obj.criteria)
        criteria_query_obj = DocsSalesCriteriaQuery(self.segment_obj.cashbox_id, criteria_data)
        query = criteria_query_obj.get_query()

        rows = await database.fetch_all(query)
        docs_sales_to_segment = [row.id for row in rows]
        await self.update_id_in_segment(docs_sales_to_segment)

        return docs_sales_to_segment

    async def collect_data(self):
        return {}

    async def run_action(self, action: str, ids: List[int], data: dict = None):
        """Метод для выполения action"""
        obj = DocsSalesAction(self.segment_obj)
        await obj.run(action, ids, data)





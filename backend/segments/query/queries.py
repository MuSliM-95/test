from sqlalchemy import select, and_

from database.db import (
    docs_sales, docs_sales_tags, OrderStatus, docs_sales_delivery_info,
    database, SegmentObjectType, contragents, contragents_tags, tags
)

from segments.query import filters as filter_query
from segments.ranges import apply_date_range, apply_range



class SegmentCriteriaQuery:

    def __init__(self, cashbox_id, criteria_data: dict):
        self.criteria_data = criteria_data
        self.base_query = (
            select(docs_sales.c.id, docs_sales.c.contragent)
            .outerjoin(contragents, docs_sales.c.contragent == contragents.c.id)
            .where(docs_sales.c.cashbox == cashbox_id)
        )
        self.filters = filter_query

    def get_query(self):
        if tag := self.criteria_data.get("tag"):
            self.base_query = (
                self.base_query
                .join(docs_sales_tags, docs_sales_tags.c.docs_sales_id == docs_sales.c.id)
                .where(docs_sales_tags.c.name == tag)
            )

        where_clauses = []  # фильтры

        if cr_at := self.criteria_data.get("created_at"):
            apply_date_range(docs_sales.c.created_at, cr_at, where_clauses)

        if self.criteria_data.get("picker"):
            self.base_query = self.filters.add_picker_filters(self.base_query, self.criteria_data.get("picker"))

        if self.criteria_data.get("courier"):
            self.base_query = self.filters.add_courier_filters(self.base_query, self.criteria_data.get("courier"))

        if "delivery_required" in self.criteria_data:
            self.base_query = self.filters.add_delivery_required_filters(self.base_query, self.criteria_data.get("delivery_required"))

        if where_clauses:
            self.base_query = self.base_query.where(and_(*where_clauses))

        if self.criteria_data.get("purchases"):
            self.base_query = self.filters.add_purchase_filters(self.base_query, self.criteria_data.get("purchases"))

        if self.criteria_data.get("loyality"):
            self.base_query = self.filters.add_loyality_filters(self.base_query, self.criteria_data.get("loyality"))

        if self.criteria_data.get("tags"):
            self.base_query = (
                self.base_query
                .outerjoin(contragents_tags, contragents_tags.c.contragent_id == contragents.c.id)
                .outerjoin(tags, tags.c.id == contragents_tags.c.tag_id)
                .where(tags.c.name.in_(self.criteria_data.get("tags")))
            )

        return self.base_query

    async def collect_ids(self):
        query = self.get_query()

        rows = await database.fetch_all(query)
        data = {
            SegmentObjectType.docs_sales.value: [],
            SegmentObjectType.contragents.value: []
        }
        for row in rows:
            data[SegmentObjectType.docs_sales.value].append(row.id)
            if not row.contragent:
                continue
            data[SegmentObjectType.contragents.value].append(row.contragent)

        data[SegmentObjectType.docs_sales.value] = set(data[SegmentObjectType.docs_sales.value])
        data[SegmentObjectType.contragents.value] = set(data[SegmentObjectType.contragents.value])
        return data

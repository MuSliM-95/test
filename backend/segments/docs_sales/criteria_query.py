from sqlalchemy import select, and_

from database.db import docs_sales, docs_sales_tags, OrderStatus

from segments.ranges import apply_date_range, apply_range
from sqlalchemy.sql import Select


class DocsSalesCriteriaQuery:

    def __init__(self, cashbox_id, criteria_data: dict):
        self.criteria_data = criteria_data
        self.base_query = (
            select(docs_sales.c.id)
            .distinct(docs_sales.c.id)
            .where(docs_sales.c.cashbox == cashbox_id)
        )

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
            self.base_query = self.add_picker_filters(self.base_query, self.criteria_data.get("picker"))

        if self.criteria_data.get("courier"):
            self.base_query = self.add_courier_filters(self.base_query, self.criteria_data.get("courier"))

        if where_clauses:
            self.base_query = self.base_query.where(and_(*where_clauses))

        return self.base_query

    @staticmethod
    def add_picker_filters(query: Select, picker_filters):

        where_clauses = []

        assigned = picker_filters.get("assigned")

        if assigned is not None:
            apply_range(docs_sales.c.assigned_picker,
                        {"is_none": not assigned}, where_clauses)

        if sdr := picker_filters.get("start"):
            apply_date_range(docs_sales.c.picker_started_at, sdr,
                             where_clauses)
        if fdr := picker_filters.get("finish"):
            apply_date_range(docs_sales.c.picker_finished_at, fdr,
                             where_clauses)

        if where_clauses:
            query = query.where(and_(*where_clauses))

        return query

    @staticmethod
    def add_courier_filters(query: Select, courier_filters):

        where_clauses = []

        assigned = courier_filters.get("assigned")

        if assigned is not None:
            apply_range(docs_sales.c.assigned_courier,
                        {"is_none": not assigned}, where_clauses)

            if assigned is False:
                apply_range(docs_sales.c.order_status,
                            {"eq": OrderStatus.collected.value}, where_clauses)

        if sdr := courier_filters.get("start"):
            apply_date_range(docs_sales.c.courier_picked_at, sdr,
                             where_clauses)
        if fdr := courier_filters.get("finish"):
            apply_date_range(docs_sales.c.courier_delivered_at, fdr,
                             where_clauses)

        if where_clauses:
            query = query.where(and_(*where_clauses))

        return query

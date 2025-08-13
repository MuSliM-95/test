from sqlalchemy import select

from database.db import (
    docs_sales, docs_sales_tags, OrderStatus, docs_sales_delivery_info, users_cboxes_relation, segments,
    database, SegmentObjectType, contragents, contragents_tags, tags, loyality_cards, loyality_transactions
)

from segments.query import filters as filter_query



class SegmentCriteriaQuery:

    def __init__(self, cashbox_id, criteria_data: dict):
        self.criteria_data = criteria_data
        self.base_query = (
            select(docs_sales.c.id, docs_sales.c.contragent)
            .outerjoin(contragents, docs_sales.c.contragent == contragents.c.id)
            .where(docs_sales.c.cashbox == cashbox_id)
        )
        self.filters = filter_query
        self.joined_tables = set()
        self.criteria_table_dependencies = {
            "picker": None,
            "courier": None,
            "purchases": None,
            "created_at": None,
            "delivery_required": ["docs_sales_delivery_info"],
            "loyality": ["loyality_cards", "loyality_transactions"],
            "tags": ["contragents_tags", "tags"],
            "docs_sales_tags": ["docs_sales_tags"],
            "delivery_info": ["docs_sales_delivery_info"]

        }
        self.table_join_configs = {
            "docs_sales_delivery_info": {
                "join_type": "outerjoin",
                "table": docs_sales_delivery_info,
                "condition": lambda: docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id
            },
            "loyality_cards": {
                "join_type": "outerjoin",
                "table": loyality_cards,
                "condition": lambda: loyality_cards.c.contragent_id == contragents.c.id
            },
            "loyality_transactions": {
                "join_type": "outerjoin",
                "table": loyality_transactions,
                "condition": lambda: loyality_transactions.c.loyality_card_id == loyality_cards.c.id,
                "depends_on": ["loyality_cards"]
            },
            "contragents_tags": {
                "join_type": "outerjoin",
                "table": contragents_tags,
                "condition": lambda: contragents_tags.c.contragent_id == contragents.c.id
            },
            "tags": {
                "join_type": "outerjoin",
                "table": tags,
                "condition": lambda: tags.c.id == contragents_tags.c.tag_id,
                "depends_on": ["contragents_tags"]
            },
            "docs_sales_tags": {
                "join_type": "outerjoin",
                "table": docs_sales_tags,
                "condition": lambda: docs_sales_tags.c.docs_sales_id == docs_sales.c.id
            },
        }
        self.criteria_handlers = {
            "picker": self.filters.add_picker_filters,
            "courier": self.filters.add_courier_filters,
            "delivery_required": self.filters.add_delivery_required_filters,
            "purchases": self.filters.add_purchase_filters,
            "loyality": self.filters.add_loyality_filters,
            "created_at": self.filters.created_at_filters,
            "tags": self.filters.tags_filters,
            "docs_sales_tags": self.filters.docs_sales_tags_filters,
            "delivery_info": self.filters.delivery_info_filters,

        }

    def get_query(self):
        """Формируем запрос"""
        required_tables = self._get_required_tables()

        # 2. Присоединяем основные таблицы
        if required_tables:
            self._add_required_joins(required_tables)

        for criterion, value in self.criteria_data.items():

            handler = self.criteria_handlers.get(criterion)
            if handler:
                self.base_query = handler(self.base_query, value)

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

    def _get_required_tables(self):
        """Только основные таблицы, подкритерии обрабатываются в фильтрах"""
        required_tables = set()

        for criterion, value in self.criteria_data.items():

            dependency = self.criteria_table_dependencies.get(criterion)
            if dependency is None:
                continue
            elif isinstance(dependency, list):
                required_tables.update(dependency)

        return required_tables

    def _add_required_joins(self, required_tables):
        """Присоединяем необходимые JOINs"""
        tables_to_join = self._sort_tables_by_dependencies(required_tables)
        for table_name in tables_to_join:
            if table_name not in self.joined_tables:
                self._add_table_join(table_name)
                self.joined_tables.add(table_name)

    def _sort_tables_by_dependencies(self, tables):
        """Сортировка присоединяемых таблиц по зависимостям"""
        sorted_tables = []
        remaining_tables = set(tables)

        while remaining_tables:
            ready_tables = []
            for table in remaining_tables:
                config = self.table_join_configs[table]
                dependencies = config.get('depends_on', [])
                if not dependencies or all(
                        dep in self.joined_tables or dep in sorted_tables for
                        dep in dependencies):
                    ready_tables.append(table)

            if not ready_tables:
                ready_tables = [list(remaining_tables)[0]]

            sorted_tables.extend(ready_tables)
            remaining_tables -= set(ready_tables)

        return sorted_tables

    def _add_table_join(self, table_name):

        config = self.table_join_configs[table_name]

        join_type = config["join_type"]
        condition = config["condition"]()
        table_obj = config["table"]

        if join_type == "outerjoin":
            self.base_query = self.base_query.outerjoin(table_obj, condition)
        elif join_type == "join":
            self.base_query = self.base_query.join(table_obj, condition)


async def get_token_by_segment_id(segment_id: int) -> str:
    """Получение токена по ID сегмента"""
    query =(
        select(users_cboxes_relation.c.token)
        .join(segments, users_cboxes_relation.c.cashbox_id == segments.c.cashbox_id)
        .where(segments.c.id == segment_id)
    )
    row = await database.fetch_one(query)
    return row.token if row else None
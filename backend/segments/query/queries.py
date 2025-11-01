from collections import defaultdict

from sqlalchemy import select

from database.db import (
    docs_sales, docs_sales_tags, OrderStatus, docs_sales_delivery_info, users_cboxes_relation, segments,
    database, SegmentObjectType, contragents, contragents_tags, tags, loyality_cards, loyality_transactions
)

from segments.query import filters as filter_query

from segments.logger import logger

FILTER_PRIORYTY_TAGS = {
    "self": 1,
    "purchase": 2,
    "delivery_info": 3,
    "docs_sales_tags": 4,
    "contragents_tags": 5,
    "loyality": 6,
}


def chunk_list(lst, chunk_size=30000):
    """Разбивает список на части заданного размера"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

class SegmentCriteriaQuery:

    def __init__(self, cashbox_id, criteria_data: dict):
        self.criteria_data = criteria_data
        self.cashbox_id = cashbox_id
        self.docs_sales_ids = []
        self.filters = filter_query
        self.criteria_config = {
            "picker": {
                "handler": self.filters.add_picker_filters,
                "filter_tag": "self",
            },
            "courier": {
                "handler": self.filters.add_courier_filters,
                "filter_tag": "self",
            },
            "delivery_required": {
                "handler": self.filters.add_delivery_required_filters,
                "filter_tag": "delivery_info",
            },
            "purchases": {
                "handler": self.filters.add_purchase_filters,
                "filter_tag": "purchase",
            },
            "loyality": {
                "handler": self.filters.add_loyality_filters,
                "filter_tag": "loyality",
            },
            "created_at": {
                "handler": self.filters.created_at_filters,
                "filter_tag": "self",
            },
            "tags": {
                "handler": self.filters.tags_filters,
                "filter_tag": "contragents_tags",
            },
            "docs_sales_tags": {
                "handler": self.filters.docs_sales_tags_filters,
                "filter_tag": "docs_sales_tags",
            },
            "delivery_info": {
                "handler": self.filters.delivery_info_filters,
                "filter_tag": "delivery_info",
            },
            "orders": {
                "handler": self.filters.orders_filters,
                "filter_tag": "self",
            },
        }

        self.filter_tag_dependencies = {
            "self": None,
            "purchase": None,
            "delivery_info": {
                "join_type": "outerjoin",
                "table": docs_sales_delivery_info,
                "condition": lambda
                    base: docs_sales_delivery_info.c.docs_sales_id == base.c.id,
            },
            "docs_sales_tags": {
                "join_type": "join",
                "table": docs_sales_tags,
                "condition": lambda
                    base: docs_sales_tags.c.docs_sales_id == base.c.id,
            },
            "contragents_tags": {
                "join_type": "join",
                "table": contragents_tags,
                "condition": lambda
                    base: contragents_tags.c.contragent_id == base.c.contragent,
            },
            "loyality": None,
        }

    def group_criteria_by_priority(self):
        """
        Группирует критерии по приоритету filter_tag.
        Возвращает список сетов, где каждый сет — это группа критериев с одинаковым приоритетом.
        """
        grouped = defaultdict(set)

        for key, value in self.criteria_data.items():
            if value in [{}, []]:
                continue
            cfg = self.criteria_config.get(key)
            if not cfg:
                continue
            tag = cfg["filter_tag"]
            priority = FILTER_PRIORYTY_TAGS.get(tag,
                                                999)  # дефолт — низший приоритет
            grouped[priority].add(key)

        # сортируем по приоритету и собираем как list[set]
        return [grouped[p] for p in sorted(grouped.keys())]

    def _add_table_join(self, subquery, tag):
        query = select(subquery.c.id, subquery.c.contragent)
        config = self.filter_tag_dependencies.get(tag)
        if not config:
            return query
        join_type = config["join_type"]
        condition = config["condition"](subquery)
        table_obj = config["table"]

        if join_type == "outerjoin":
            return query.outerjoin(table_obj, condition)
        elif join_type == "join":
            return query.join(table_obj, condition)

    async def calculate(self):
        """Собираем Id документов продаж"""
        docs_sales_rows = await database.fetch_all(select(docs_sales.c.id).where(docs_sales.c.cashbox == self.cashbox_id, docs_sales.c.is_deleted == False))
        self.docs_sales_ids = [row.id for row in docs_sales_rows]
        groups = self.group_criteria_by_priority()
        for group in groups:
            if not self.docs_sales_ids:
                return []

            tag = self.criteria_config.get(list(group)[0]).get("filter_tag",
                                                               "self")

            # Обрабатываем ID частями
            all_filtered_rows = []

            for chunk_num, chunk_ids in enumerate(
                    chunk_list(self.docs_sales_ids, 30000)):
                # Создаем подзапрос для текущей части
                subq = (
                    select(docs_sales)
                    .where(docs_sales.c.id.in_(chunk_ids))
                    .subquery("sub")
                )

                # Добавляем джоины
                query = self._add_table_join(subq, tag)

                # Применяем обработчики критериев
                for criteria in group:
                    data = self.criteria_data.get(criteria)
                    handler = self.criteria_config.get(criteria, {}).get(
                        "handler")
                    if handler:
                        query = handler(query, data, subq)
                # Выполняем запрос для части
                try:
                    chunk_rows = await database.fetch_all(query)
                    all_filtered_rows.extend(chunk_rows)
                except Exception as e:
                    logger.error(f"Error processing chunk {chunk_num + 1}: {e}")
                    raise

            # Обновляем список ID результатами из всех частей
            docs_sales_rows = all_filtered_rows
            self.docs_sales_ids = [row.id for row in docs_sales_rows]

        return self.docs_sales_ids

    async def collect_ids(self):
        docs_sales_ids = await self.calculate()

        data = {
            SegmentObjectType.docs_sales.value: set(),
            SegmentObjectType.contragents.value: set()
        }

        # Обрабатываем ID частями
        for chunk_num, chunk_ids in enumerate(
                chunk_list(docs_sales_ids, 30000)):

            # Создаем запрос для текущей части
            query = select(docs_sales.c.id, docs_sales.c.contragent).where(
                docs_sales.c.id.in_(chunk_ids)
            )

            try:
                chunk_rows = await database.fetch_all(query)

                # Обрабатываем результаты части
                for row in chunk_rows:
                    data[SegmentObjectType.docs_sales.value].add(row.id)
                    if row.contragent:
                        data[SegmentObjectType.contragents.value].add(
                            row.contragent)

            except Exception as e:
                logger.error(f"Error processing chunk {chunk_num + 1}: {e}")
                raise

        return data


async def get_token_by_segment_id(segment_id: int) -> str:
    """Получение токена по ID сегмента"""
    query =(
        select(users_cboxes_relation.c.token)
        .join(segments, users_cboxes_relation.c.cashbox_id == segments.c.cashbox_id)
        .where(segments.c.id == segment_id)
    )
    row = await database.fetch_one(query)
    return row.token if row else None

async def fetch_contragent_by_id(cid):
    row = await database.fetch_one(
        select([contragents.c.name, contragents.c.phone])
        .where(contragents.c.id == cid)
    )
    return row

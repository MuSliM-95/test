import json
from datetime import datetime, timedelta

from database.db import (
    segments, database, contragents, docs_sales, docs_sales_goods,
    nomenclature, categories, loyality_cards, loyality_transactions,
    client_segments, client_segment_history, SegmentStatusHistory, SegmentStatus
)
from sqlalchemy import func, and_, text, or_, select

from segments.ranges import apply_range, apply_date_range
from sqlalchemy.sql import Select

from segments.base_logic import BaseSegmentLogic


class ContragentsLogic(BaseSegmentLogic):

    def add_purchase_filters(self, query: Select, purchase_criteria: dict) -> Select:
        """
            Добавляет в запрос фильтры и агрегаты, описанные в purchase_criteria.

            :param query: исходный Select (contragents JOIN docs_sales … DISTINCT)
            :param purchase_criteria: словарь из PurchaseCriteria
            :return: модифицированный Select
        """
        where_clauses = []  # фильтры по каждой продаже
        having_clauses = []  # агрегаты по контрагенту

        # ---- 1. Диапазон дат конкретных чеков ----------------------------------
        if dr := purchase_criteria.get("date_range"):
            apply_date_range(docs_sales.c.created_at, dr, where_clauses)

        # ---- 2. Сумма одного чека ----------------------------------------------
        if per_check := purchase_criteria.get("amount_per_check"):
            apply_range(docs_sales.c.sum, per_check, where_clauses)

        if purchase_criteria.get("categories") or purchase_criteria.get("nomenclatures"):
            query = (
                query
                .outerjoin(docs_sales_goods,
                      docs_sales_goods.c.docs_sales_id == docs_sales.c.id)
                .outerjoin(nomenclature,
                      docs_sales_goods.c.nomenclature == nomenclature.c.id)
                .outerjoin(categories, nomenclature.c.category == categories.c.id)
            )

        # ---- 3. Категории товаров ----------------------------------------------
        if cats := purchase_criteria.get("categories"):

            like_conditions = [
                categories.c.name.ilike(f"%{cat}%") for cat in cats
            ]

            where_clauses.append(or_(*like_conditions))

        if nomenclatures := purchase_criteria.get("nomenclatures"):

            like_conditions = [
                nomenclature.c.name.ilike(f"%{nom}%") for nom in nomenclatures
            ]

            where_clauses.append(or_(*like_conditions))

        # ---- 4. Агрегаты (COUNT / SUM) -----------------------------------------
        # Нам понадобятся GROUP BY contragent.id
        query = query.group_by(contragents.c.id)

        if rng := purchase_criteria.get("count"):
            apply_range(func.count(docs_sales.c.id), rng, having_clauses)

        if rng := purchase_criteria.get("total_amount"):
            apply_range(func.sum(docs_sales.c.sum), rng, having_clauses)

        # ---- 5. Последняя покупка N дней назад ---------------------------------
        if rng := purchase_criteria.get("last_purchase_days_ago"):
            # func.max(docs_sales.c.date) – дата последней покупки
            max_date = func.max(docs_sales.c.created_at)

            if "gte" in rng:  # ≥ N дней назад  → дата ≤ now - N
                cutoff = datetime.utcnow() - timedelta(days=rng["gte"])
                having_clauses.append(max_date <= cutoff)

            if "lte" in rng:  # ≤ N дней назад  → дата ≥ now - N
                cutoff = datetime.utcnow() - timedelta(days=rng["lte"])
                having_clauses.append(max_date >= cutoff)

        # ---- 6. Применяем всё к запросу ----------------------------------------
        if where_clauses:
            query = query.where(and_(*where_clauses))

        if having_clauses:
            query = query.having(and_(*having_clauses))

        return query

    def add_loyality_filters(self, query: Select, loyality_criteria: dict) -> Select:
        """
            Добавляет в запрос фильтры и агрегаты, описанные в loyality_criteria.

            :param query: исходный Select (contragents JOIN docs_sales … DISTINCT)
            :param loyality_criteria: словарь из LoyalityCriteria
            :return: модифицированный Select
        """

        where_clauses = []

        query = (
            query
            .outerjoin(loyality_cards, loyality_cards.c.contragent_id == contragents.c.id)
            .outerjoin(loyality_transactions, loyality_transactions.c.loyality_card_id == loyality_cards.c.id)
        )

        if balance := loyality_criteria.get("balance"):
            apply_range(loyality_cards.c.balance, balance, where_clauses)

        if expire := loyality_criteria.get("expires_in_days"):
            # INTERVAL '1 second' * lifetime
            expiry_datetime = loyality_transactions.c.created_at + text(
                "INTERVAL '1 second'") * loyality_cards.c.lifetime

            # Остаток в днях
            days_left = func.DATE_PART('day', expiry_datetime - func.now())

            apply_range(days_left, expire, where_clauses)

        if where_clauses:
            query = query.where(and_(*where_clauses))

        return query

    async def update_segment(self):

        criteria_data = json.loads(self.segment_obj.criteria)

        query = (
            select(contragents.c.id)
            .distinct(contragents.c.id)
            .join(docs_sales, docs_sales.c.contragent == contragents.c.id)
            .where(docs_sales.c.cashbox == self.segment_obj.cashbox_id)
        )

        if criteria_data.get("purchases"):
            query = self.add_purchase_filters(query, criteria_data.get("purchases"))

        if criteria_data.get("loyality"):
            query = self.add_loyality_filters(query, criteria_data.get("loyality"))
        rows = await database.fetch_all(query)
        contragent_to_segment = [row.id for row in rows]
        contragent_in_segment = await self.get_contragent_in_segment()
        ids_to_segment = list(set(contragent_to_segment) - set(contragent_in_segment))
        ids_out_of_segment = list(set(contragent_in_segment) - set(contragent_to_segment))
        if ids_to_segment:
            await self.add_contragents_to_segment(ids_to_segment)
        if ids_out_of_segment:
            await self.remove_contragents_from_segment(ids_out_of_segment)

        return contragent_to_segment

    async def get_contragent_in_segment(self):
        query = select(client_segments.c.contragent_id).where(client_segments.c.segment_id == self.segment_obj.id)
        contragent_ids = await database.fetch_all(query)
        return [row.contragent_id for row in contragent_ids]

    async def add_contragents_to_segment(self, contragent_ids: list) -> None:
        query = client_segments.insert()
        history_query = client_segment_history.insert()

        values = []
        history_values = []
        for contragent_id in contragent_ids:
            values.append({"segment_id": self.segment_obj.id, "contragent_id": contragent_id})
            history_values.append({
                "segment_id": self.segment_obj.id,
                "contragent_id": contragent_id,
                "status": SegmentStatusHistory.added.value
            })

        await database.execute_many(query=query, values=values)
        await database.execute_many(query=history_query, values=history_values)

    async def remove_contragents_from_segment(self, contragent_ids: list) -> None:
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

    async def start_actions(self):
        return

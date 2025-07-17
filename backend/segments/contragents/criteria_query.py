from datetime import datetime, timedelta

from sqlalchemy import select, or_, func, and_, text

from database.db import (
    contragents, docs_sales, docs_sales_goods, nomenclature, categories,
    loyality_cards, loyality_transactions, contragents_tags, tags
)
from sqlalchemy.sql import Select

from segments.ranges import apply_date_range, apply_range


class ContragentsCriteriaQuery:

    def __init__(self, cashbox_id, criteria_data: dict):
        self.criteria_data = criteria_data
        self.base_query = (
            select(contragents.c.id)
            .distinct(contragents.c.id)
            .join(docs_sales, docs_sales.c.contragent == contragents.c.id)
            .where(docs_sales.c.cashbox == cashbox_id)
        )

    def get_query(self):
        if self.criteria_data.get("purchases"):
            self.base_query = self.add_purchase_filters(self.base_query, self.criteria_data.get("purchases"))

        if self.criteria_data.get("loyality"):
            self.base_query = self.add_loyality_filters(self.base_query, self.criteria_data.get("loyality"))

        if self.criteria_data.get("tags"):
            self.base_query = (
                self.base_query
                .outerjoin(contragents_tags, contragents_tags.c.contragent_id == contragents.c.id)
                .outerjoin(tags, tags.c.id == contragents_tags.c.tag_id)
                .where(tags.c.name.in_(self.criteria_data.get("tags")))
            )

        return self.base_query

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
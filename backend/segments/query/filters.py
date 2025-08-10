from datetime import datetime, timedelta

from sqlalchemy import and_, or_, func, select, text
from sqlalchemy.sql import Select

from database.db import (
    docs_sales, OrderStatus, docs_sales_delivery_info, docs_sales_goods,
    nomenclature, categories, contragents, loyality_cards,
    loyality_transactions, contragents_tags, tags, docs_sales_tags
)
from segments.ranges import apply_range, apply_date_range


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


def add_delivery_required_filters(query: Select, delivery_required: bool):

    if delivery_required is True:
        query = query.where(docs_sales_delivery_info.c.docs_sales_id.isnot(None))
    elif delivery_required is False:
        query = query.where(docs_sales_delivery_info.c.docs_sales_id.is_(None))

    return query


def add_purchase_filters(query: Select, purchase_criteria: dict) -> Select:
    """
        Добавляет в запрос фильтры и агрегаты, описанные в purchase_criteria.

        :param query: исходный Select (docs_sales outerjoin(contragents))
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

    subq_having = []

    subq = (
        select(docs_sales.c.contragent)
        .group_by(docs_sales.c.contragent)
    )

    # count документов на контрагента
    if rng := purchase_criteria.get("count"):
        apply_range(func.count(docs_sales.c.id), rng, subq_having)

    # сумма документов на контрагента
    if rng := purchase_criteria.get("total_amount"):
        apply_range(func.sum(docs_sales.c.sum), rng, subq_having)

    if subq_having:
        subq = subq.having(and_(*subq_having))

    subq = subq.subquery()

    query = (
        query
        .where(docs_sales.c.contragent.in_(select(subq.c.contragent)))
    )


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
        query = query.group_by(docs_sales.c.id)
        query = query.having(and_(*having_clauses))

    return query


def add_loyality_filters(query: Select, loyality_criteria: dict) -> Select:
    """
        Добавляет в запрос фильтры и агрегаты, описанные в loyality_criteria.

        :param query: исходный Select (contragents JOIN docs_sales … DISTINCT)
        :param loyality_criteria: словарь из LoyalityCriteria
        :return: модифицированный Select
    """

    where_clauses = []

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


def created_at_filters(query: Select, data: dict) -> Select:
    where_clauses = []
    apply_date_range(docs_sales.c.created_at, data, where_clauses)
    if where_clauses:
        query = query.where(and_(*where_clauses))
    return query


def tags_filters(query: Select, data: list) -> Select:
    like_conditions = [
        tags.c.name.ilike(f"%{tag}%") for tag in data
    ]
    return (
        query
        .where(or_(*like_conditions))
    )


def docs_sales_tags_filters(query: Select, data: list) -> Select:
    like_conditions = [
        docs_sales_tags.c.name.ilike(f"%{tag}%") for tag in data
    ]
    return (
        query
        .where(or_(*like_conditions))
    )


def delivery_info_filters(query: Select, data: dict) -> Select:
    where_clauses = []

    if dd := data.get("delivery_date"):
        apply_date_range(docs_sales_delivery_info.c.delivery_date, dd, where_clauses)

    if address := data.get("address"):
        where_clauses.append(docs_sales_delivery_info.c.address.ilike(f"%{address}%"))

    if note := data.get("note"):
        where_clauses.append(
            docs_sales_delivery_info.c.note.ilike(f"%{note}%"))

    if recipient := data.get("recipient"):
        if not isinstance(recipient, dict):
            recipient = {}
        for k, v in recipient.items():
            where_clauses.append(
                docs_sales_delivery_info.c.recipient.op('->>')(text(f"'{k}'")).ilike(f'%{v}%')
            )

    if where_clauses:
        query = query.where(and_(*where_clauses))

    return query

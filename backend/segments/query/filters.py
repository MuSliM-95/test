from datetime import datetime, timedelta

from sqlalchemy import and_, or_, func, select, text, exists
from sqlalchemy.sql import Select

from database.db import (
    docs_sales, OrderStatus, docs_sales_delivery_info, docs_sales_goods,
    nomenclature, categories, contragents, loyality_cards,
    loyality_transactions, contragents_tags, tags, docs_sales_tags, pictures
)
from segments.ranges import apply_range, apply_date_range


def add_picker_filters(query: Select, picker_filters, sub):

    where_clauses = []

    assigned = picker_filters.get("assigned")

    photos_not_added_minutes = picker_filters.get("photos_not_added_minutes")

    if photos_not_added_minutes is not None and isinstance(photos_not_added_minutes, int):
        sales_entity = "docs_sales"

        pictures_of_sales = pictures.c.entity == sales_entity
        pictures_of_order = pictures.c.entity_id == sub.c.id

        # year,month,week,days,hour,min,sec
        interval = func.make_interval(0, 0, 0, 0, 0, photos_not_added_minutes, 0)

        picker_finished = sub.c.picker_finished_at.isnot(None)

        not_any_photo = ~exists(
            select(1).where(and_(
                pictures_of_sales,
                pictures_of_order
            )))

        picker_late_photos = func.now() >= sub.c.picker_finished_at + interval

        photo_filter = and_(picker_finished, not_any_photo, picker_late_photos)

        where_clauses.append(photo_filter)

    if assigned is not None:
        apply_range(sub.c.assigned_picker,
                    {"is_none": not assigned}, where_clauses)

    if sdr := picker_filters.get("start"):
        apply_date_range(sub.c.picker_started_at, sdr,
                         where_clauses)
    if fdr := picker_filters.get("finish"):
        apply_date_range(sub.c.picker_finished_at, fdr,
                         where_clauses)

    if where_clauses:
        query = query.where(and_(*where_clauses))

    return query

def add_courier_filters(query: Select, courier_filters, sub):

    where_clauses = []

    assigned = courier_filters.get("assigned")

    if assigned is not None:
        apply_range(sub.c.assigned_courier,
                    {"is_none": not assigned}, where_clauses)

        if assigned is False:
            apply_range(sub.c.order_status,
                        {"eq": OrderStatus.collected.value}, where_clauses)

    if sdr := courier_filters.get("start"):
        apply_date_range(sub.c.courier_picked_at, sdr,
                         where_clauses)
    if fdr := courier_filters.get("finish"):
        apply_date_range(sub.c.courier_delivered_at, fdr,
                         where_clauses)

    if where_clauses:
        query = query.where(and_(*where_clauses))

    return query


def add_delivery_required_filters(query: Select, delivery_required: bool, sub):

    if delivery_required is True:
        query = query.where(and_(
            docs_sales_delivery_info.c.docs_sales_id.isnot(None),
            docs_sales_delivery_info.c.address.isnot(None),
            docs_sales_delivery_info.c.address != ""
        ))
    elif delivery_required is False:
        query = query.where(or_(
            docs_sales_delivery_info.c.docs_sales_id.is_(None),
            docs_sales_delivery_info.c.address.is_(None),
            docs_sales_delivery_info.c.address == ""
        ))

    return query


def add_purchase_filters(query: Select, purchase_criteria: dict, sub) -> Select:
    """
        Добавляет в запрос фильтры и агрегаты, описанные в purchase_criteria.

        :param query: исходный Select (на sub)
        :param purchase_criteria: словарь из PurchaseCriteria
        :param sub: подзапрос с docs_sales.id, docs_sales.contragent
        :return: модифицированный Select
    """
    where_clauses = []   # фильтры по чекам
    having_clauses = []  # агрегаты по чекам

    # ---- 1. Диапазон дат по чеку ---------------------------------
    if dr := purchase_criteria.get("date_range"):
        apply_date_range(sub.c.created_at, dr, where_clauses)

    # ---- 2. Сумма одного чека -----------------------------------
    if per_check := purchase_criteria.get("amount_per_check"):
        apply_range(sub.c.sum, per_check, where_clauses)

    # ---- 3. Фильтр по категориям / номенклатурам через EXISTS ----
    if cats := purchase_criteria.get("categories"):
        for cat in cats:
            exists_clause = (
                select(1)
                .select_from(docs_sales_goods)
                .join(nomenclature, docs_sales_goods.c.nomenclature == nomenclature.c.id)
                .join(categories, nomenclature.c.category == categories.c.id)
                .where(docs_sales_goods.c.docs_sales_id == sub.c.id)
                .where(categories.c.name.ilike(f"%{cat}%"))
            )
            where_clauses.append(exists(exists_clause))

    if noms := purchase_criteria.get("nomenclatures"):
        for nom in noms:
            exists_clause = (
                select(1)
                .select_from(docs_sales_goods)
                .join(nomenclature, docs_sales_goods.c.nomenclature == nomenclature.c.id)
                .where(docs_sales_goods.c.docs_sales_id == sub.c.id)
                .where(nomenclature.c.name.ilike(f"%{nom}%"))
            )
            where_clauses.append(exists(exists_clause))

    # ---- 4. Агрегаты по контрагенту (COUNT / SUM) -----------------
    subq = (
        select(docs_sales.c.contragent)
        .group_by(docs_sales.c.contragent)
    )

    if rng := purchase_criteria.get("count"):
        apply_range(func.count(docs_sales.c.id), rng, having_clauses)

    if rng := purchase_criteria.get("total_amount"):
        apply_range(func.sum(docs_sales.c.sum), rng, having_clauses)

    if having_clauses:
        subq = subq.having(and_(*having_clauses))

    subq = subq.subquery()

    query = query.where(sub.c.contragent.in_(select(subq.c.contragent)))

    # ---- 5. Последняя покупка N дней назад -----------------------
    if rng := purchase_criteria.get("last_purchase_days_ago"):
        max_date = func.max(docs_sales.c.created_at)

        sub_last = (
            select(docs_sales.c.contragent, max_date.label("last_purchase"))
            .group_by(docs_sales.c.contragent)
            .subquery()
        )

        query = query.join(
            sub_last, sub_last.c.contragent == sub.c.contragent
        )

        if "gte" in rng:
            cutoff = datetime.utcnow() - timedelta(days=rng["gte"])
            query = query.where(sub_last.c.last_purchase <= cutoff)

        if "lte" in rng:
            cutoff = datetime.utcnow() - timedelta(days=rng["lte"])
            query = query.where(sub_last.c.last_purchase >= cutoff)

    # ---- 6. Применяем фильтры -----------------------------------
    if where_clauses:
        query = query.where(and_(*where_clauses))

    # гарантируем уникальные чеки
    query = query.distinct(sub.c.id)

    return query


def add_loyality_filters(query: Select, loyality_criteria: dict, sub) -> Select:
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
        query = query.join(loyality_transactions, loyality_transactions.c.loyality_card_id == loyality_cards.c.id)
        expiry_datetime = loyality_transactions.c.created_at + text(
            "INTERVAL '1 second'") * loyality_cards.c.lifetime

        # Остаток в днях
        days_left = func.DATE_PART('day', expiry_datetime - func.now())

        apply_range(days_left, expire, where_clauses)

    if where_clauses:
        query = query.where(and_(*where_clauses))

    return query


def created_at_filters(query: Select, data: dict, sub) -> Select:
    where_clauses = []
    apply_date_range(sub.c.created_at, data, where_clauses)
    if where_clauses:
        query = query.where(and_(*where_clauses))
    return query


def tags_filters(query: Select, data: list, sub) -> Select:
    like_conditions = [
        tags.c.name.ilike(f"%{tag}%") for tag in data
    ]
    return (
        query
        .where(or_(*like_conditions))
    )


def docs_sales_tags_filters(query: Select, data: list, sub) -> Select:
    like_conditions = [
        docs_sales_tags.c.name.ilike(f"%{tag}%") for tag in data
    ]
    return (
        query
        .where(or_(*like_conditions))
    )


def delivery_info_filters(query: Select, data: dict, sub) -> Select:
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

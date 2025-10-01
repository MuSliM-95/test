from collections import defaultdict

from sqlalchemy import case, select, func, and_

from database.db import (
    warehouse_register_movement, price_types, nomenclature, database, prices,
    categories, pictures, nomenclature_attributes_value, nomenclature_attributes
)


class FeedCriteriaFilter:

    def __init__(self, criteria_data: dict, cashbox_id):
        self.criteria_data = criteria_data
        self.cashbox_id = cashbox_id

    def add_filters(self, query, q):
        """Добавляем фильтры к запросу"""
        criteria = self.criteria_data

        if criteria.get("warehouse_id"):
            query = query.where(
                warehouse_register_movement.c.warehouse_id.in_(criteria["warehouse_id"])
            )

        if criteria.get("category_id"):
            query = query.where(
                nomenclature.c.category.in_(criteria["category_id"])
            )

        if criteria.get("prices"):
            if criteria["prices"].get("from"):
                query = query.where(prices.c.price >= criteria["prices"]["from"])
            if criteria["prices"].get("to"):
                query = query.where(prices.c.price <= criteria["prices"]["to"])

        if criteria.get("only_on_stock"):
            query = query.having(func.sum(q) > 0)

        return query

    async def get_warehouse_balance(self):
        price_type_id = self.criteria_data.get("price_types_id")
        if not price_type_id:
            query = (
                select(price_types)
                .where(price_types.c.cashbox == self.cashbox_id)
            )
            types = await database.fetch_all(query)
            price_type_id = types[0].id if types else None

        if not price_type_id:
            return None

        # считаем остаток (учёт минус/плюс)
        q = case(
            (warehouse_register_movement.c.type_amount == "minus", warehouse_register_movement.c.amount * -1),
            else_=warehouse_register_movement.c.amount,
        )

        # базовый запрос
        query = (
            select(
                nomenclature.c.id.label("id"),
                nomenclature.c.name.label("name"),
                categories.c.name.label("category"),
                nomenclature.c.description_short.label("description"),
                prices.c.price.label("price"),
                warehouse_register_movement.c.warehouse_id.label("warehouse_id"),
                func.sum(q).label("current_amount"),
            )
            .join(
                nomenclature,
                warehouse_register_movement.c.nomenclature_id == nomenclature.c.id,
            )
            .join(
                prices,
                and_(
                    prices.c.nomenclature == nomenclature.c.id,
                    prices.c.price_type == price_type_id,
                ),
            )
            .join(
                categories,
                categories.c.id == nomenclature.c.category,
            )
            .where(warehouse_register_movement.c.cashbox_id == self.cashbox_id)
            .group_by(
                nomenclature.c.id,
                warehouse_register_movement.c.organization_id,
                warehouse_register_movement.c.warehouse_id,
                categories.c.name,
                prices.c.price
            )
        )

        # применяем фильтры
        query = self.add_filters(query, q)

        # выполняем запрос
        rows = await database.fetch_all(query)
        results = [dict(row) for row in rows]

        if not results:
            return []

        # достаём все id номенклатур
        nomenclature_ids = [r["id"] for r in results]

        # тянем картинки пачкой
        images_query = (
            select(
                pictures.c.entity_id,
                pictures.c.url
            )
            .where(
                and_(
                    pictures.c.entity == "nomenclature",
                    pictures.c.entity_id.in_(nomenclature_ids)
                )
            )
        )
        images_rows = await database.fetch_all(images_query)

        # группируем картинки по номенклатуре
        images_map = defaultdict(list)
        for row in images_rows:
            images_map[row.entity_id].append(f"https://app.tablecrm.com/api/v1/{row.url}")

        # ---- атрибуты ----
        attrs_query = (
            select(
                nomenclature_attributes_value.c.nomenclature_id,
                nomenclature_attributes.c.name,
                nomenclature_attributes_value.c.value,
            )
            .join(
                nomenclature_attributes,
                nomenclature_attributes.c.id == nomenclature_attributes_value.c.attribute_id,
            )
            .where(
                and_(
                    nomenclature_attributes_value.c.nomenclature_id.in_(
                        nomenclature_ids),
                    nomenclature_attributes.c.cashbox == self.cashbox_id,
                )
            )
        )
        attrs_rows = await database.fetch_all(attrs_query)
        attrs_map = defaultdict(dict)
        for row in attrs_rows:
            attrs_map[row.nomenclature_id][row.name] = row.value

        # ---- собираем финальный результат ----
        for r in results:
            r["images"] = images_map.get(r["id"], None)
            r["params"] = attrs_map.get(r["id"], None)

        return results
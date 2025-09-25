from sqlalchemy import case, select, func, and_

from database.db import warehouse_register_movement, price_types, nomenclature, database, prices, categories


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
        return [dict(row) for row in rows]
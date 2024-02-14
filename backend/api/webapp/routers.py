from database.db import (pictures, price_types, warehouse_balances, categories,
                         prices, nomenclature, database, warehouses, manufacturers,
                         warehouse_register_movement, units, organizations)
from typing import Optional
from datetime import datetime
from fastapi import APIRouter, Depends
from functions.helpers import (
    datetime_to_timestamp,
    get_user_by_token,
    nomenclature_unit_id_to_name,
)
from sqlalchemy import func, select, desc, case
from functions.filter_schemas import *
import api.webapp.schemas as schemas

router = APIRouter(tags=["webapp"])


@router.get("/webapp/")
async def get_nomenclature(
        token: str,
        warehouse_id: Optional[int] = None,
        nomenclature_id: Optional[int] = None,
        organization_id: Optional[int] = None,
        name: Optional[str] = None,
        date_from: Optional[int] = None,
        date_to: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
        filter_pictures: PicturesFiltersQuery = Depends(),
        filter_prices: PricesFiltersQuery = Depends(),
):
    """Получение фотографий, цен и их видов, остатков и названия категорий"""

    user = await get_user_by_token(token)

    filters = [
        nomenclature.c.cashbox == user.cashbox_id,
        nomenclature.c.is_deleted.is_not(True),
    ]

    if name:
        filters.append(nomenclature.c.name.ilike(f"%{name}%"))

    query = nomenclature.select().where(*filters).limit(limit).offset(offset)

    nomenclature_db = await database.fetch_all(query)
    nomenclature_db = [*map(datetime_to_timestamp, nomenclature_db)]
    nomenclature_db = [*map(nomenclature_unit_id_to_name, nomenclature_db)]
    nomenclature_db = [await inst for inst in nomenclature_db]

    query = select(func.count(nomenclature.c.id)).where(*filters)
    nomenclature_db_c = await database.fetch_one(query)

    for item in nomenclature_db:
        filter_pictures_list = []
        if filter_pictures.entity:
            filter_pictures_list.append(pictures.c.entity == filter_pictures.entity)
        if filter_pictures.entity_id:
            filter_pictures_list.append(pictures.c.entity_id == filter_pictures.entity_id)

        query = pictures.select().where(pictures.c.entity_id == item['id'],
                                        pictures.c.owner == user.id,
                                        pictures.c.is_deleted.is_not(True),
                                        *filter_pictures_list)
        pictures_db = await database.fetch_all(query)
        pictures_db = [*map(datetime_to_timestamp, pictures_db)]
        item['pictures'] = pictures_db

        # query = prices.select().where(prices.c.nomenclature == item['id'])
        # price_db = await database.fetch_one(query)
        #
        # if price_db is not None:
        #     query = price_types.select().where(price_types.c.id == price_db.price_type,
        #                                        price_types.c.owner == user.id,
        #                                        price_types.c.is_deleted.is_not(True))
        #     price_types_db = await database.fetch_all(query)
        #     price_types_db = [*map(datetime_to_timestamp, price_types_db)]
        # else:
        #     price_types_db = []
        # item['price_types'] = price_types_db

        filter_prices_nom = []
        filter_prices_price = []
        if filter_prices.name:
            filter_prices_nom.append(nomenclature.c.name.ilike(f"%{filter_prices.name}%"))
        if filter_prices.type:
            filter_prices_nom.append(nomenclature.c.type == filter_prices.type)
        if filter_prices.description_short:
            filter_prices_nom.append(nomenclature.c.description_short.ilike(f"%{filter_prices.description_short}%"))
        if filter_prices.description_long:
            filter_prices_nom.append(nomenclature.c.description_long.ilike(f"%{filter_prices.description_long}%"))
        if filter_prices.code:
            filter_prices_nom.append(nomenclature.c.code == filter_prices.code)
        if filter_prices.unit:
            filter_prices_nom.append(nomenclature.c.unit == filter_prices.unit)
        if filter_prices.category:
            filter_prices_nom.append(nomenclature.c.category.in_(filter_prices.category.split(",")))
        if filter_prices.manufacturer:
            filter_prices_nom.append(nomenclature.c.manufacturer == filter_prices.manufacturer)
        if filter_prices.price_type_id:
            filter_prices_price.append(prices.c.price_type == filter_prices.price_type_id)
        if filter_prices.date_from:
            filter_prices_price.append(prices.c.price_type >= filter_prices.date_from)
        if filter_prices.date_to:
            filter_prices_price.append(prices.c.date_to <= filter_prices.date_to)
        q = (prices.select().where(prices.c.owner == user.id, prices.c.is_deleted == False, *filter_prices_price)
                   .order_by(desc(prices.c.id))
             )
        prices_db = await database.fetch_all(q)

        response_body_list = []
        for price_db in prices_db:
            if price_db.nomenclature != item['id']:
                continue
            response_body = {**dict(price_db)}

            response_body["id"] = price_db.id
            response_body["price"] = price_db.price
            response_body["date_to"] = price_db.date_to
            response_body["date_from"] = price_db.date_from
            response_body["updated_at"] = price_db.updated_at
            response_body["created_at"] = price_db.created_at

            q = nomenclature.select().where(

                nomenclature.c.id == price_db.nomenclature,
                nomenclature.c.owner == user.id,
                nomenclature.c.is_deleted == False,
                *filter_prices_nom,
            )
            nom_db = await database.fetch_one(q)

            if price_db.price_type:
                q = price_types.select().where(price_types.c.id == price_db.price_type)
                price_type = await database.fetch_one(q)

                if price_type:
                    response_body["price_type"] = price_type.name

            if nom_db:

                if nom_db.unit:
                    q = units.select().where(units.c.id == nom_db.unit)
                    unit = await database.fetch_one(q)

                    if unit:
                        response_body["unit_name"] = unit.name

                    if nom_db.category:
                        q = categories.select().where(categories.c.id == nom_db.category)
                        category = await database.fetch_one(q)

                        if category:
                            response_body["category_name"] = category.name

                    if nom_db.manufacturer:
                        q = manufacturers.select().where(manufacturers.c.id == nom_db.manufacturer)
                        manufacturer = await database.fetch_one(q)

                        if manufacturer:
                            response_body["manufacturer_name"] = manufacturer.name

            else:
                continue

            response_body = datetime_to_timestamp(response_body)
            response_body_list.append(response_body)
        for price in response_body_list:
            query = price_types.select().where(price_types.c.name == price['price_type'],
                                               price_types.c.owner == user.id,
                                               price_types.c.is_deleted.is_not(True))
            price_types_db = await database.fetch_all(query)
            price_types_db = [*map(datetime_to_timestamp, price_types_db)]
            del price['price_type']
            price['price_types'] = price_types_db
        item['prices'] = response_body_list

        dates_arr = []
        if date_to and not date_from:
            dates_arr.append(warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to))
        if date_to and date_from:
            dates_arr.append(warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to))
            dates_arr.append(warehouse_register_movement.c.created_at >= datetime.fromtimestamp(date_from))
        if not date_to and date_from:
            dates_arr.append(warehouse_register_movement.c.created_at >= datetime.fromtimestamp(date_from))

        selection_conditions = [warehouse_register_movement.c.nomenclature_id == item['id'], *dates_arr]
        if organization_id is not None:
            selection_conditions.append(warehouse_register_movement.c.organization_id == organization_id)
        q = case(
            [
                (
                    warehouse_register_movement.c.type_amount == 'minus',
                    warehouse_register_movement.c.amount * (-1)
                )
            ],
            else_=warehouse_register_movement.c.amount

        )
        query = (
            select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.category,
                warehouse_register_movement.c.organization_id,
                warehouse_register_movement.c.warehouse_id,
                func.sum(q).label("current_amount"))
            .where(*selection_conditions)
            .limit(limit)
            .offset(offset)
        ).group_by(
            nomenclature.c.name,
            nomenclature.c.id,
            warehouse_register_movement.c.organization_id,
            warehouse_register_movement.c.warehouse_id
        ) \
            .select_from(warehouse_register_movement
                         .join(nomenclature,
                               warehouse_register_movement.c.nomenclature_id == nomenclature.c.id
                               ))

        warehouse_balances_db = await database.fetch_all(query)

        selection_conditions = [warehouse_register_movement.c.nomenclature_id == item['id']]
        if organization_id is not None:
            selection_conditions.append(warehouse_register_movement.c.organization_id == organization_id)
        q = case(
            [
                (
                    warehouse_register_movement.c.type_amount == 'minus',
                    warehouse_register_movement.c.amount * (-1)
                )
            ],
            else_=warehouse_register_movement.c.amount

        )
        query = (
            select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.category,
                warehouse_register_movement.c.organization_id,
                warehouse_register_movement.c.warehouse_id,
                func.sum(q).label("current_amount"))
            .where(*selection_conditions)
            .limit(limit)
            .offset(offset)
        ).group_by(
            nomenclature.c.name,
            nomenclature.c.id,
            warehouse_register_movement.c.organization_id,
            warehouse_register_movement.c.warehouse_id
        ) \
            .select_from(warehouse_register_movement
                         .join(nomenclature,
                               warehouse_register_movement.c.nomenclature_id == nomenclature.c.id
                               ))

        warehouse_balances_db_curr = await database.fetch_all(query)

        # warehouse_balances_db = [*map(datetime_to_timestamp, warehouse_balances_db)]
        res = []

        categories_db = await database.fetch_all(categories.select())

        res_with_cats = []

        for warehouse_balance in warehouse_balances_db:

            current = [item for item in warehouse_balances_db_curr if item.id == warehouse_balance.id]

            balance_dict = dict(warehouse_balance)
            return balance_dict
            organization_db = await database.fetch_one(
                organizations.select().where(organizations.c.id == warehouse_balance.organization_id))

            plus_amount = 0
            minus_amount = 0

            register_q = warehouse_register_movement.select().where(
                warehouse_register_movement.c.warehouse_id == warehouse_id,
                warehouse_register_movement.c.nomenclature_id == warehouse_balance.id,
                *dates_arr
            ) \
                .order_by(warehouse_register_movement.c.id)

            register_events = await database.fetch_all(register_q)

            for reg_event in register_events:
                if reg_event.type_amount == "plus":
                    plus_amount += reg_event.amount
                else:
                    minus_amount += reg_event.amount

            balance_dict['organization_name'] = organization_db.short_name
            balance_dict['plus_amount'] = plus_amount
            balance_dict['minus_amount'] = minus_amount
            balance_dict['start_ost'] = balance_dict['current_amount'] - plus_amount + minus_amount
            balance_dict['now_ost'] = current[0].current_amount
            filter_warehouses = [
                warehouses.c.cashbox == user.cashbox_id,
                warehouses.c.is_deleted.is_not(True),
            ]

            query = warehouses.select().where(warehouses.c.id == warehouse_balance.warehouse_id,
                                              *filter_warehouses)

            warehouses_db = await database.fetch_all(query)
            warehouses_db = [*map(datetime_to_timestamp, warehouses_db)]
            balance_dict['warehouses'] = warehouses_db

            res.append(balance_dict)
        for category in categories_db:
            cat_childrens = []
            for item_cat in res:
                if item_cat['category'] == category.id:
                    cat_childrens.append(item_cat)

            if len(cat_childrens) > 0:
                res_with_cats.append(
                    {
                        "name": category.name,
                        "key": category.id,
                        "children": cat_childrens
                    }
                )

        none_childrens = [item for item in res if item['category'] == None]
        res_with_cats.append(
            {
                "name": "Без категории",
                "key": 0,
                "children": none_childrens
            }
        )
        item['alt_warehouse_balances'] = res_with_cats

    return {"result": nomenclature_db, "count": nomenclature_db_c.count_1}

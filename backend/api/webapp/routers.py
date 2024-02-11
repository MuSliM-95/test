from database.db import (pictures, price_types, warehouse_balances,
                         prices, nomenclature, database, warehouses, warehouse_register_movement)
from typing import Optional
from datetime import datetime
from fastapi import APIRouter, Depends
from functions.helpers import (
    datetime_to_timestamp,
    get_user_by_token,
    nomenclature_unit_id_to_name,
)
from sqlalchemy import func, select
from functions.filter_schemas import *
from api.webapp.schemas import WebappItem

router = APIRouter(tags=["webapp"])


@router.get("/webapp/", response_model=WebappItem)
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
            # filter_pictures_list = []
            # if filter_pictures.entity:
            #     filter_pictures_list.append(pictures.c.entity == filter_pictures.entity)
            # if filter_pictures.entity_id:
            #     filter_pictures_list.append(pictures.c.entity_id == filter_pictures.entity_id)

        query = pictures.select().where(pictures.c.entity_id == item['id'])
        pictures_db = await database.fetch_all(query)
        item['pictures'] = pictures_db

        # query = price_types.select().where(price_types.c.id == item['id'])
        # price_types_db = await database.fetch_all(query)
        # item['price_types'] = price_types_db
        #
        # filter_prices_nom = []
        # filter_prices_price = []
        # if filter_prices.name:
        #     filter_prices_nom.append(nomenclature.c.name.ilike(f"%{filter_prices.name}%"))
        # if filter_prices.type:
        #     filter_prices_nom.append(nomenclature.c.type == filter_prices.type)
        # if filter_prices.description_short:
        #     filter_prices_nom.append(nomenclature.c.description_short.ilike(f"%{filter_prices.description_short}%"))
        # if filter_prices.description_long:
        #     filter_prices_nom.append(nomenclature.c.description_long.ilike(f"%{filter_prices.description_long}%"))
        # if filter_prices.code:
        #     filter_prices_nom.append(nomenclature.c.code == filter_prices.code)
        # if filter_prices.unit:
        #     filter_prices_nom.append(nomenclature.c.unit == filter_prices.unit)
        # if filter_prices.category:
        #     filter_prices_nom.append(nomenclature.c.category.in_(filter_prices.category.split(",")))
        # if filter_prices.manufacturer:
        #     filter_prices_nom.append(nomenclature.c.manufacturer == filter_prices.manufacturer)
        # if filter_prices.price_type_id:
        #     filter_prices_price.append(prices.c.price_type == filter_prices.price_type_id)
        # if filter_prices.date_from:
        #     filter_prices_price.append(prices.c.price_type >= filter_prices.date_from)
        # if filter_prices.date_to:
        #     filter_prices_price.append(prices.c.date_to <= filter_prices.date_to)
        # query = prices.select().where(prices.c.nomenclature == item['id'],
        #                               *filter_prices_price)
        # prices_db = await database.fetch_all(query)
        # item['prices'] = prices_db
        #
        # dates_arr = []
        # if date_to and not date_from:
        #     dates_arr.append(warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to))
        # if date_to and date_from:
        #     dates_arr.append(warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to))
        #     dates_arr.append(warehouse_register_movement.c.created_at >= datetime.fromtimestamp(date_from))
        # if not date_to and date_from:
        #     dates_arr.append(warehouse_register_movement.c.created_at >= datetime.fromtimestamp(date_from))
        #
        # selection_conditions = [warehouse_register_movement.c.warehouse_id == warehouse_id, *dates_arr]
        # if nomenclature_id is not None:
        #     selection_conditions.append(warehouse_register_movement.c.nomenclature_id == nomenclature_id)
        # if organization_id is not None:
        #     selection_conditions.append(warehouse_register_movement.c.organization_id == organization_id)
        # query = warehouse_balances.select().where(warehouse_balances.c.nomenclature_id == item['id'],
        #                                           *selection_conditions)
        # alt_warehouse_balances_db = await database.fetch_all(query)
        # item['alt_warehouse_balances'] = alt_warehouse_balances_db
        #
        # filter_warehouses = [
        #     warehouses.c.cashbox == user.cashbox_id,
        #     warehouses.c.is_deleted.is_not(True),
        # ]
        # if name:
        #     filter_warehouses.append(
        #         warehouses.c.name.ilike(f"%{name}%"),
        #     )
        # query = warehouses.select().where(warehouses.c.id == item['id'],
        #                                   *filter_warehouses)
        # warehouses_db = await database.fetch_all(query)
        # item['warehouses'] = warehouses_db

    return {"result": nomenclature_db, "count": nomenclature_db_c.count_1}

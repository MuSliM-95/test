from database.db import (pictures, price_types, warehouse_balances, prices, nomenclature, database, warehouses)
from typing import Optional
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


@router.get("/webapp/")
async def get_nomenclature(
        token: str,
        name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        filters_pictures: PicturesFiltersQuery = Depends(),
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
        filters_pictures_list = []
        if filters_pictures.entity:
            filters_pictures_list.append(pictures.c.entity == filters_pictures.entity)
        if filters_pictures.entity_id:
            filters_pictures_list.append(pictures.c.entity_id == filters_pictures.entity_id)

        query = pictures.select().where(pictures.c.entity_id == item['id'],
                                        *filters_pictures_list)
        pictures_db = await database.fetch_all(query)
        item['pictures'] = pictures_db

        query = price_types.select().where(price_types.c.id == item['id'])
        price_types_db = await database.fetch_all(query)
        item['price_types'] = price_types_db

        query = prices.select().where(prices.c.nomenclature == item['id'])
        prices_db = await database.fetch_all(query)
        item['prices'] = prices_db

        query = warehouse_balances.select().where(warehouse_balances.c.nomenclature_id == item['id'])
        alt_warehouse_balances_db = await database.fetch_all(query)
        item['alt_warehouse_balances'] = alt_warehouse_balances_db

        query = warehouses.select().where(warehouses.c.id == item['id'])
        warehouses_db = await database.fetch_all(query)
        item['warehouses'] = warehouses_db

    return {"result": nomenclature_db, "count": nomenclature_db_c.count_1}

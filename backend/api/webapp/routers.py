from typing import Optional
from database.db import database, pictures, price_types, warehouse_balances, prices
from api.nomenclature.routers import get_nomenclature
from fastapi import APIRouter

from typing import List, Optional

import api.nomenclature.schemas as schemas
from database.db import categories, database, manufacturers, nomenclature
from fastapi import APIRouter, HTTPException
from functions.helpers import (
    check_entity_exists,
    check_unit_exists,
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
    nomenclature_unit_id_to_name,
)
from sqlalchemy import func, select
from ws_manager import manager

router = APIRouter(tags=["webapp"])


@router.get("/webapp/")
async def get_nomenclature(
        token: str,
        name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
):
    """Получение фотографий, цен и их видов, остатков и названия категорий"""

    response = await get_nomenclature(token, name, limit, offset)
    nomenclature_db = response['result']

    # user = await get_user_by_token(token)
    #
    # filters = [
    #     nomenclature.c.cashbox == user.cashbox_id,
    #     nomenclature.c.is_deleted.is_not(True),
    # ]
    #
    # if name:
    #     filters.append(nomenclature.c.name.ilike(f"%{name}%"))
    #
    # query = nomenclature.select().where(*filters).limit(limit).offset(offset)
    #
    # nomenclature_db = await database.fetch_all(query)
    # nomenclature_db = [*map(datetime_to_timestamp, nomenclature_db)]
    # nomenclature_db = [*map(nomenclature_unit_id_to_name, nomenclature_db)]
    # nomenclature_db = [await inst for inst in nomenclature_db]
    #
    # query = select(func.count(nomenclature.c.id)).where(*filters)
    # nomenclature_db_c = await database.fetch_one(query)

    for item in nomenclature_db:
        query = pictures.select().where(pictures.c.entity_id == item['id'])
        # print(item['id'])
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

    return {"result": nomenclature_db, "count": nomenclature_db_c.count_1}

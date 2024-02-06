from typing import Optional
import api.webapp.schemas as schemas
from database.db import database, pictures, price_types, warehouse_balances, prices
from fastapi import APIRouter

router = APIRouter(tags=["webapp"])


@router.get("/webapp/", response_model=schemas.NomenclatureListGetRes)
async def get_nomenclature(
        token: str,
        name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
):
    """Получение фотографий, цен и их видов, остатков и названия категорий"""

    from api.nomenclature.routers import get_nomenclature
    response = await get_nomenclature(token, name, limit, offset)
    nomenclature_db = response['result']


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

    return {"result": nomenclature_db, "count": response['count']}

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


@router.get("/nomenclature/", response_model=schemas.NomenclatureListGetRes)
async def get_nomenclature(token: str, name: Optional[str] = None, limit: int = 1000, offset: int = 0):
    """Получение фотографий, цен, остатков, категорий"""
    user = await get_user_by_token(token)

    filters = [
        nomenclature.c.owner == user.id,
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

    return {"result": nomenclature_db, "count": nomenclature_db_c.count_1}

from fastapi import APIRouter, HTTPException

from database.db import database, units

import api.units.schemas as schemas

router = APIRouter(tags=["units"])


@router.get("/units/", response_model=schemas.UnitList)
async def get_available_units(limit: int = 100, offset: int = 0):
    """Получение списка доступных единиц измерения"""
    query = units.select().limit(limit).offset(offset)
    return await database.fetch_all(query)


@router.get("/units/{idx}", response_model=schemas.Unit)
async def get_unit(idx: int):
    """Получение единицы измерения"""
    query = units.select().where(units.c.id == idx)
    entity_db = await database.fetch_one(query)

    if not entity_db:
        raise HTTPException(status_code=404, detail=f"У вас нет ед. изм. с таким id")

    return entity_db

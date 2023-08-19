from asyncpg import IntegrityConstraintViolationError
from fastapi import APIRouter, HTTPException

from database.db import database, manufacturers

import api.manufacturers.schemas as schemas

from functions.helpers import datetime_to_timestamp, get_entity_by_id
from functions.helpers import get_user_by_token

from ws_manager import manager

router = APIRouter(tags=["manufacturers"])


@router.get("/manufacturers/{idx}", response_model=schemas.Manufacturer)
async def get_manufacturer_by_id(token: str, idx: int):
    """Получение производителя по ID"""
    user = await get_user_by_token(token)
    manufacturer_db = await get_entity_by_id(manufacturers, idx, user.id)
    manufacturer_db = datetime_to_timestamp(manufacturer_db)
    return manufacturer_db


@router.get("/manufacturers/", response_model=schemas.ManufacturerList)
async def get_manufacturers(token: str, limit: int = 100, offset: int = 0):
    """Получение списка производителей"""
    user = await get_user_by_token(token)
    query = (
        manufacturers.select()
        .where(
            manufacturers.c.owner == user.id,
            manufacturers.c.is_deleted.is_not(True),
        )
        .limit(limit)
        .offset(offset)
    )

    manufacturers_db = await database.fetch_all(query)
    manufacturers_db = [*map(datetime_to_timestamp, manufacturers_db)]

    return manufacturers_db


@router.post("/manufacturers/", response_model=schemas.ManufacturerList)
async def new_manufacturers(token: str, manufacturers_data: schemas.ManufacturerCreateMass):
    """Создание производителя"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    for manufacturer_values in manufacturers_data.dict()["__root__"]:
        manufacturer_values["owner"] = user.id

        query = manufacturers.insert().values(manufacturer_values)
        manufacturer_id = await database.execute(query)
        inserted_ids.add(manufacturer_id)

    query = (
        manufacturers.select()
        .where(
            manufacturers.c.owner == user.id,
            manufacturers.c.id.in_(inserted_ids)
        )
    )
    manufacturers_db = await database.fetch_all(query)
    manufacturers_db = [*map(datetime_to_timestamp, manufacturers_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "manufacturers",
            "result": manufacturers_db,
        },
    )

    return manufacturers_db


@router.patch("/manufacturers/{idx}", response_model=schemas.Manufacturer)
async def edit_manufacturer(
    token: str,
    idx: int,
    manufacturer: schemas.ManufacturerEdit,
):
    """Редактирование производителя"""
    user = await get_user_by_token(token)
    manufacturer_db = await get_entity_by_id(manufacturers, idx, user.id)
    manufacturer_values = manufacturer.dict(exclude_unset=True)

    if manufacturer_values:
        query = (
            manufacturers.update()
            .where(manufacturers.c.id == idx, manufacturers.c.owner == user.id)
            .values(manufacturer_values)
        )
        await database.execute(query)
        manufacturer_db = await get_entity_by_id(manufacturers, idx, user.id)

    manufacturer_db = datetime_to_timestamp(manufacturer_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "manufacturers", "result": manufacturer_db},
    )

    return manufacturer_db


@router.delete("/manufacturers/{idx}", response_model=schemas.Manufacturer)
async def delete_manufacturer(token: str, idx: int):
    """Удаление производителя"""
    user = await get_user_by_token(token)

    await get_entity_by_id(manufacturers, idx, user.id)

    query = (
        manufacturers.update()
        .where(manufacturers.c.id == idx, manufacturers.c.owner == user.id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = manufacturers.select().where(
        manufacturers.c.id == idx, manufacturers.c.owner == user.id
    )
    manufacturer_db = await database.fetch_one(query)
    manufacturer_db = datetime_to_timestamp(manufacturer_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "manufacturers",
            "result": manufacturer_db,
        },
    )

    return manufacturer_db

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

router = APIRouter(tags=["nomenclature"])


@router.get("/nomenclature/{idx}/", response_model=schemas.NomenclatureGet)
async def get_nomenclature_by_id(token: str, idx: int):
    """Получение категории по ID"""
    user = await get_user_by_token(token)
    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.id)
    nomenclature_db = datetime_to_timestamp(nomenclature_db)
    nomenclature_db = await nomenclature_unit_id_to_name(nomenclature_db)
    return nomenclature_db


@router.get("/nomenclature/", response_model=schemas.NomenclatureListGetRes)
async def get_nomenclature(token: str, name: Optional[str] = None, limit: int = 100, offset: int = 0):
    """Получение списка категорий"""
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


@router.post("/nomenclature/", response_model=schemas.NomenclatureList)
async def new_nomenclature(token: str, nomenclature_data: schemas.NomenclatureCreateMass):
    """Создание категории"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    categories_cache = set()
    manufacturers_cache = set()
    units_cache = set()
    exceptions = []
    for nomenclature_values in nomenclature_data.dict()["__root__"]:
        nomenclature_values["cashbox"] = user.cashbox_id
        nomenclature_values["owner"] = user.id
        nomenclature_values["is_deleted"] = False

        if nomenclature_values.get("category") is not None:
            if nomenclature_values["category"] not in categories_cache:
                try:
                    await check_entity_exists(categories, nomenclature_values["category"], user.cashbox_id)
                    categories_cache.add(nomenclature_values["category"])
                except HTTPException as e:
                    exceptions.append(str(nomenclature_values) + " " + e.detail)
                    continue

        if nomenclature_values.get("manufacturer") is not None:
            if nomenclature_values["manufacturer"] not in manufacturers_cache:
                try:
                    await check_entity_exists(manufacturers, nomenclature_values["manufacturer"], user.id)
                    manufacturers_cache.add(nomenclature_values["manufacturer"])
                except HTTPException as e:
                    exceptions.append(str(nomenclature_values) + " " + e.detail)
                    continue

        if nomenclature_values.get("unit") is not None:
            if nomenclature_values["unit"] not in units_cache:
                try:
                    await check_unit_exists(nomenclature_values["unit"])
                    units_cache.add(nomenclature_values["unit"])
                except HTTPException as e:
                    exceptions.append(str(nomenclature_values) + " " + e.detail)
                    continue

        query = nomenclature.insert().values(nomenclature_values)
        nomenclature_id = await database.execute(query)
        inserted_ids.add(nomenclature_id)

    query = nomenclature.select().where(nomenclature.c.owner == user.id, nomenclature.c.id.in_(inserted_ids))
    nomenclature_db = await database.fetch_all(query)
    nomenclature_db = [*map(datetime_to_timestamp, nomenclature_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "nomenclature",
            "result": nomenclature_db,
        },
    )

    if exceptions:
        raise HTTPException(400, "Не были добавлены следующие записи: " + ", ".join(exceptions))

    return nomenclature_db


@router.patch("/nomenclature/{idx}/", response_model=schemas.Nomenclature)
async def edit_nomenclature(
    token: str,
    idx: int,
    nomenclature_data: schemas.NomenclatureEdit,
):
    """Редактирование категории"""
    user = await get_user_by_token(token)
    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.id)
    nomenclature_values = nomenclature_data.dict(exclude_unset=True)

    if nomenclature_values:
        if nomenclature_values.get("category") is not None:
            await check_entity_exists(categories, nomenclature_values["category"], user.id)
        if nomenclature_values.get("manufacturer") is not None:
            await check_entity_exists(manufacturers, nomenclature_values["manufacturer"], user.id)
        if nomenclature_values.get("unit") is not None:
            await check_unit_exists(nomenclature_values["unit"])

        query = (
            nomenclature.update()
            .where(nomenclature.c.id == idx, nomenclature.c.owner == user.id)
            .values(nomenclature_values)
        )
        await database.execute(query)
        nomenclature_db = await get_entity_by_id(nomenclature, idx, user.id)

    nomenclature_db = datetime_to_timestamp(nomenclature_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "nomenclature", "result": nomenclature_db},
    )

    return nomenclature_db


@router.patch("/nomenclature/", response_model=List[schemas.Nomenclature])
async def edit_nomenclature_mass(
    token: str,
    nomenclature_data: List[schemas.NomenclatureEditMass],
):
    """Редактирование номенклатуры пачкой"""
    user = await get_user_by_token(token)
    response_body = []
    for nomenclature_in_list in nomenclature_data:
        idx = nomenclature_in_list.id
        nomenclature_db = await get_entity_by_id(nomenclature, idx, user.id)
        nomenclature_values = nomenclature_in_list.dict(exclude_unset=True)

        del nomenclature_values["id"]

        if nomenclature_values:
            if nomenclature_values.get("category") is not None:
                await check_entity_exists(categories, nomenclature_values["category"], user.id)
            if nomenclature_values.get("manufacturer") is not None:
                await check_entity_exists(manufacturers, nomenclature_values["manufacturer"], user.id)
            if nomenclature_values.get("unit") is not None:
                await check_unit_exists(nomenclature_values["unit"])

            query = (
                nomenclature.update()
                .where(nomenclature.c.id == idx, nomenclature.c.owner == user.id)
                .values(nomenclature_values)
            )
            await database.execute(query)
            nomenclature_db = await get_entity_by_id(nomenclature, idx, user.id)

        nomenclature_db = datetime_to_timestamp(nomenclature_db)

        await manager.send_message(
            token,
            {"action": "edit", "target": "nomenclature", "result": nomenclature_db},
        )

        response_body.append(nomenclature_db)

    return response_body


@router.delete("/nomenclature/{idx}/", response_model=schemas.Nomenclature)
async def delete_nomenclature(token: str, idx: int):
    """Удаление категории"""
    user = await get_user_by_token(token)

    await get_entity_by_id(nomenclature, idx, user.id)

    query = (
        nomenclature.update()
        .where(nomenclature.c.id == idx, nomenclature.c.owner == user.id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = nomenclature.select().where(nomenclature.c.id == idx, nomenclature.c.owner == user.id)
    nomenclature_db = await database.fetch_one(query)
    nomenclature_db = datetime_to_timestamp(nomenclature_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "nomenclature",
            "result": nomenclature_db,
        },
    )

    return nomenclature_db


@router.delete("/nomenclature/", response_model=List[schemas.Nomenclature])
async def delete_nomenclature_mass(token: str, nomenclature_data: List[int]):
    """Удаление категории пачкой"""
    user = await get_user_by_token(token)

    response_body = []

    for idx in nomenclature_data:
        await get_entity_by_id(nomenclature, idx, user.id)

        query = (
            nomenclature.update()
            .where(nomenclature.c.id == idx, nomenclature.c.owner == user.id)
            .values({"is_deleted": True})
        )
        await database.execute(query)

        query = nomenclature.select().where(nomenclature.c.id == idx, nomenclature.c.owner == user.id)
        nomenclature_db = await database.fetch_one(query)
        nomenclature_db = datetime_to_timestamp(nomenclature_db)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "nomenclature",
                "result": nomenclature_db,
            },
        )

        response_body.append(nomenclature_db)

    return response_body

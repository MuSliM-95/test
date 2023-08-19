from fastapi import APIRouter

from database.db import database, price_types

import api.price_types.schemas as schemas

from functions.helpers import datetime_to_timestamp, get_entity_by_id
from functions.helpers import get_user_by_token

from ws_manager import manager

router = APIRouter(tags=["price_types"])


@router.get("/price_types/{idx}", response_model=schemas.PriceType)
async def get_price_type_by_id(token: str, idx: int):
    """Получение типа цен по ID"""
    user = await get_user_by_token(token)
    price_type_db = await get_entity_by_id(price_types, idx, user.id)
    price_type_db = datetime_to_timestamp(price_type_db)
    return price_type_db


@router.get("/price_types/", response_model=schemas.PriceTypeList)
async def get_price_types(token: str, limit: int = 100, offset: int = 0):
    """Получение списка типов цен"""
    user = await get_user_by_token(token)
    query = (
        price_types.select()
        .where(
            price_types.c.owner == user.id,
            price_types.c.is_deleted.is_not(True),
        )
        .limit(limit)
        .offset(offset)
    )

    price_types_db = await database.fetch_all(query)
    price_types_db = [*map(datetime_to_timestamp, price_types_db)]

    return price_types_db


@router.post("/price_types/", response_model=schemas.PriceType)
async def new_price_type(token: str, price_type: schemas.PriceTypeCreate):
    """Создание типа цен"""
    user = await get_user_by_token(token)

    price_type_values = price_type.dict()
    price_type_values["owner"] = user.id

    query = price_types.insert().values(price_type_values)
    price_type_id = await database.execute(query)

    price_type_db = await get_entity_by_id(price_types, price_type_id, user.id)
    price_type_db = datetime_to_timestamp(price_type_db)

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "price_types",
            "result": price_type_db,
        },
    )

    return price_type_db


@router.patch("/price_types/{idx}", response_model=schemas.PriceType)
async def edit_price_type(
    token: str,
    idx: int,
    price_type: schemas.PriceTypeEdit,
):
    """Редактирование типа цен"""
    user = await get_user_by_token(token)
    price_type_db = await get_entity_by_id(price_types, idx, user.id)
    price_type_values = price_type.dict(exclude_unset=True)

    if price_type_values:
        query = (
            price_types.update()
            .where(price_types.c.id == idx, price_types.c.owner == user.id)
            .values(price_type_values)
        )
        await database.execute(query)
        price_type_db = await get_entity_by_id(price_types, idx, user.id)

    price_type_db = datetime_to_timestamp(price_type_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "price_types", "result": price_type_db},
    )

    return price_type_db


@router.delete("/price_types/{idx}", response_model=schemas.PriceType)
async def delete_price_type(token: str, idx: int):
    """Удаление типа цен"""
    user = await get_user_by_token(token)

    await get_entity_by_id(price_types, idx, user.id)

    query = (
        price_types.update()
        .where(price_types.c.id == idx, price_types.c.owner == user.id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = price_types.select().where(
        price_types.c.id == idx, price_types.c.owner == user.id
    )
    price_type_db = await database.fetch_one(query)
    price_type_db = datetime_to_timestamp(price_type_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "price_types",
            "result": price_type_db,
        },
    )

    return price_type_db

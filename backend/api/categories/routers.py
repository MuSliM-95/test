from asyncpg import ForeignKeyViolationError, IntegrityConstraintViolationError
from fastapi import APIRouter, HTTPException

from database.db import database, categories

import api.categories.schemas as schemas

from functions.helpers import datetime_to_timestamp, get_entity_by_id, check_entity_exists
from functions.helpers import get_user_by_token

from ws_manager import manager

router = APIRouter(tags=["categories"])


@router.get("/categories/{idx}", response_model=schemas.Category)
async def get_category_by_id(token: str, idx: int):
    """Получение категории по ID"""
    user = await get_user_by_token(token)
    category_db = await get_entity_by_id(categories, idx, user.id)
    category_db = datetime_to_timestamp(category_db)
    return category_db


@router.get("/categories/", response_model=schemas.CategoryList)
async def get_categories(token: str, limit: int = 100, offset: int = 0):
    """Получение списка категорий"""
    user = await get_user_by_token(token)
    query = (
        categories.select()
        .where(
            categories.c.owner == user.id,
            categories.c.is_deleted.is_not(True),
        )
        .limit(limit)
        .offset(offset)
    )

    categories_db = await database.fetch_all(query)
    categories_db = [*map(datetime_to_timestamp, categories_db)]

    return categories_db


@router.post("/categories/", response_model=schemas.CategoryList)
async def new_categories(token: str, categories_data: schemas.CategoryCreateMass):
    """Создание категорий"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    parents_cache = set()
    exceptions = []
    for category_values in categories_data.dict()["__root__"]:
        category_values["owner"] = user.id
        category_values["cashbox"] = user.cashbox_id

        if category_values.get("parent") is not None:
            if category_values["parent"] not in parents_cache:
                try:
                    await check_entity_exists(categories, category_values["parent"], user.id)
                    parents_cache.add(category_values["parent"])
                except HTTPException as e:
                    exceptions.append(str(category_values) + " " + e.detail)
                    continue

        query = categories.insert().values(category_values)
        category_id = await database.execute(query)
        inserted_ids.add(category_id)

    query = (
        categories.select()
        .where(
            categories.c.owner == user.id,
            categories.c.id.in_(inserted_ids)
        )
    )
    categories_db = await database.fetch_all(query)
    categories_db = [*map(datetime_to_timestamp, categories_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "categories",
            "result": categories_db,
        },
    )

    if exceptions:
        raise HTTPException(400, "Не были добавлены следующие записи: " + ", ".join(exceptions))

    return categories_db


@router.patch("/categories/{idx}", response_model=schemas.Category)
async def edit_category(
    token: str,
    idx: int,
    category: schemas.CategoryEdit,
):
    """Редактирование категории"""
    user = await get_user_by_token(token)
    category_db = await get_entity_by_id(categories, idx, user.id)
    category_values = category.dict(exclude_unset=True)

    if category_values:
        if category_values.get("parent") is not None:
            await check_entity_exists(categories, category_values["parent"], user.id)

        query = (
            categories.update()
            .where(categories.c.id == idx, categories.c.owner == user.id)
            .values(category_values)
        )
        await database.execute(query)
        category_db = await get_entity_by_id(categories, idx, user.id)

    category_db = datetime_to_timestamp(category_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "categories", "result": category_db},
    )

    return category_db


@router.delete("/categories/{idx}", response_model=schemas.Category)
async def delete_category(token: str, idx: int):
    """Удаление категории"""
    user = await get_user_by_token(token)

    await get_entity_by_id(categories, idx, user.id)

    query = (
        categories.update()
        .where(categories.c.id == idx, categories.c.owner == user.id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = categories.select().where(
        categories.c.id == idx, categories.c.owner == user.id
    )
    category_db = await database.fetch_one(query)
    category_db = datetime_to_timestamp(category_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "categories",
            "result": category_db,
        },
    )

    return category_db

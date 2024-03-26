import api.categories.schemas as schemas
from asyncpg import ForeignKeyViolationError, IntegrityConstraintViolationError
from database.db import categories, database, nomenclature
from fastapi import APIRouter, HTTPException
from functions.helpers import check_entity_exists, datetime_to_timestamp, get_entity_by_id, get_user_by_token
from sqlalchemy import func, select
from ws_manager import manager
import asyncio
from typing import Optional

router = APIRouter(tags=["categories"])


@router.get("/categories/{idx}/", response_model=schemas.Category)
async def get_category_by_id(token: str, idx: int):
    """Получение категории по ID"""
    user = await get_user_by_token(token)
    category_db = await get_entity_by_id(categories, idx, user.id)
    category_db = datetime_to_timestamp(category_db)
    return category_db


@router.get("/categories/", response_model=schemas.CategoryListGet)
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

    query = select(func.count(categories.c.id)).where(
        categories.c.owner == user.id,
        categories.c.is_deleted.is_not(True),
    )

    categories_db_count = await database.fetch_one(query)

    return {"result": categories_db, "count": categories_db_count.count_1}

async def build_hierarchy(data, parent_id = None, name = None):
    async def build_children(parent_id):
        children = []
        for item in data:
            item = datetime_to_timestamp(item)
            item['children'] = []
            item['key'] = item['id']

            if name is not None:
                nomenclature_in_category = await database.fetch_all(
                    nomenclature.select().
                    where( nomenclature.c.name.ilike(f"%{name}%"),
                           nomenclature.c.category == item.get("id")))
                item["nom_count"] = len(nomenclature_in_category)

            else:
                item["nom_count"] = 0

            item['expanded_flag'] = False
            if item['parent'] == parent_id:
                grandchildren = await build_children(item['id'])
                if grandchildren:
                    item['children'] = grandchildren
                print(dict(item))
                if (item['nom_count'] == 0) and (name is not None):
                    print('continue')
                    continue
                children.append(item)
        return children
    
    tasks = [build_children(parent_id)]
    results = await asyncio.gather(*tasks)
    return results[0]


@router.get("/categories_tree/", response_model=schemas.CategoryTreeGet)
async def get_categories(token: str, nomenclature_name: Optional[str] = None):
    """Получение древа списка категорий"""
    user = await get_user_by_token(token)
    query = (
        categories.select()
        .where(
            categories.c.owner == user.id,
            categories.c.is_deleted.is_not(True),
            categories.c.parent == None
        )
    )

    categories_db = await database.fetch_all(query)
    result = []

    nomenclature_list = []
    if nomenclature_name != None:
        q = nomenclature.select().where(nomenclature.c.name.ilike(f"%{nomenclature_name}%"), nomenclature.c.category != None)
        nomenclature_list = await database.fetch_all(q)

    for category in categories_db:
        category_dict = dict(category)
        category_dict['key'] = category_dict['id']

        if nomenclature_name is not None:
            nomenclature_in_category = await database.fetch_all(
                nomenclature.select().
                where(nomenclature.c.name.ilike(f"%{nomenclature_name}%"), nomenclature.c.category == category.get("id")))
            category_dict["nom_count"] = len(nomenclature_in_category)
        else:
            category_dict["nom_count"] = 0

        category_dict['expanded_flag'] = False
        query = (
            f"""
                with recursive categories_hierarchy as (
                select id, name, parent, description, code, status, updated_at, created_at
                from categories where parent = {category.id}

                union
                select F.id, F.name, F.parent, F.description, F.code, F.status, F.updated_at, F.created_at
                from categories_hierarchy as H
                join categories as F on F.parent = H.id
                ) 
                select * from categories_hierarchy 
            """
        )
        childrens = await database.fetch_all(query)
        if childrens:
            category_dict['children'] = await build_hierarchy([dict(child) for child in childrens], category.id, nomenclature_name)

            def count_nomeclature(data, s):
                res = 0
                for item in data:
                    print(item)
                    if item['children']:
                        print(item['nom_count'])
                        res = + s
                        count_nomeclature(item['children'], item['nom_count'])
                    else:
                        continue
                return res

            category_dict["nom_count"] = count_nomeclature(category_dict['children'], category_dict["nom_count"])
        else:
            category_dict['children'] = []
        
        flag = True
        if nomenclature_name != None:
            flag = False
            cats_ids = [child.parent for child in childrens]
            if len(childrens) != 0:
                cats_ids.append(childrens[-1].id)
            else:
                cats_ids = [category.id]
            for nomenclature_entity in nomenclature_list:
                if nomenclature_entity.category in cats_ids:
                    flag = True
                    break

        if flag is True:
            result.append(category_dict)

    categories_db = [*map(datetime_to_timestamp, result)]


    query = select(func.count(categories.c.id)).where(
        categories.c.owner == user.id,
        categories.c.is_deleted.is_not(True),
    )

    categories_db_count = await database.fetch_one(query)
    return {"result": categories_db, "count": categories_db_count.count_1}

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

    query = categories.select().where(categories.c.owner == user.id, categories.c.id.in_(inserted_ids))
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


@router.patch("/categories/{idx}/", response_model=schemas.Category)
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

        query = categories.update().where(categories.c.id == idx, categories.c.owner == user.id).values(category_values)
        await database.execute(query)
        category_db = await get_entity_by_id(categories, idx, user.id)

    category_db = datetime_to_timestamp(category_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "categories", "result": category_db},
    )

    return category_db


@router.delete("/categories/{idx}/", response_model=schemas.Category)
async def delete_category(token: str, idx: int):
    """Удаление категории"""
    user = await get_user_by_token(token)

    await get_entity_by_id(categories, idx, user.id)

    query = (
        categories.update().where(categories.c.id == idx, categories.c.owner == user.id).values({"is_deleted": True})
    )
    await database.execute(query)

    query = categories.select().where(categories.c.id == idx, categories.c.owner == user.id)
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

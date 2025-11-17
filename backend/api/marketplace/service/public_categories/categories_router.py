"""Маркетплейс: публичные категории (роутер)"""
import uuid
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, File, HTTPException, UploadFile
from sqlalchemy import func, select

from .category import (GlobalCategoryCreate, GlobalCategoryList,
                       GlobalCategoryTree, GlobalCategoryTreeList,
                       GlobalCategoryUpdate)
from .database import database, global_categories
from .helpers import serialize_datetime_fields

router = APIRouter(prefix="/mp/categories", tags=["categories"])


async def build_global_hierarchy(
    categories_data: List[dict], parent_id: Optional[int] = None
) -> List[dict]:
    result = []
    for category in categories_data:
        if category.get("parent_id") == parent_id:
            category_dict = dict(category)
            children = await build_global_hierarchy(
                categories_data, category["id"]
            )
            category_dict["children"] = children
            result.append(category_dict)
    return result


@router.get("/", response_model=GlobalCategoryList)
async def get_global_categories(limit: int = 100, offset: int = 0):
    query = (
        select(global_categories)
        .where(global_categories.c.is_active.is_(True))
        .order_by(global_categories.c.name)
        .limit(limit)
        .offset(offset)
    )
    categories_db = await database.fetch_all(query)
    categories_db = [*map(serialize_datetime_fields, categories_db)]
    count_query = select(func.count(global_categories.c.id)).where(
        global_categories.c.is_active.is_(True)
    )
    categories_count = await database.fetch_one(count_query)
    return {"result": categories_db, "count": categories_count.count_1}


@router.get("/tree/", response_model=GlobalCategoryTreeList)
async def get_global_categories_tree():
    query = (
        select(global_categories)
        .where(global_categories.c.is_active.is_(True))
        .order_by(global_categories.c.name)
    )
    categories_db = await database.fetch_all(query)
    categories_db = [*map(serialize_datetime_fields, categories_db)]
    tree = await build_global_hierarchy(categories_db, parent_id=None)
    count_query = select(func.count(global_categories.c.id)).where(
        global_categories.c.is_active.is_(True)
    )
    categories_count = await database.fetch_one(count_query)
    return {"result": tree, "count": categories_count.count_1}


@router.get("/{category_id}/", response_model=GlobalCategoryTree)
async def get_global_category(category_id: int):
    query = select(global_categories).where(
        global_categories.c.id == category_id,
        global_categories.c.is_active.is_(True),
    )
    category = await database.fetch_one(query)
    if not category:
        raise HTTPException(status_code=404, detail="Категория не найдена")
    category_dict = dict(serialize_datetime_fields(category))
    # Получаем дерево для этой категории
    children_query = select(global_categories).where(
        global_categories.c.parent_id == category_id,
        global_categories.c.is_active.is_(True),
    )
    children = await database.fetch_all(children_query)
    category_dict["children"] = [
        dict(serialize_datetime_fields(child))
        for child in children
    ]
    return category_dict


@router.post("/", response_model=GlobalCategoryTree, status_code=201)
async def create_global_category(category: GlobalCategoryCreate):
    insert_query = global_categories.insert().values(**category.model_dump())
    new_category_id = await database.execute(insert_query)
    created_category_query = select(global_categories).where(
        global_categories.c.id == new_category_id
    )
    created_category = await database.fetch_one(created_category_query)
    created_category_dict = dict(serialize_datetime_fields(created_category))
    created_category_dict["children"] = []
    return created_category_dict


@router.patch("/{category_id}/", response_model=GlobalCategoryTree)
async def update_global_category(
    category_id: int,
    category_update: GlobalCategoryUpdate
):
    """
    Обновление категории
    Можно обновить только указанные поля, остальные остаются без изменений
    """
    check_query = select(global_categories).where(
        global_categories.c.id == category_id,
        global_categories.c.is_active.is_(True),
    )
    existing_category = await database.fetch_one(check_query)
    if not existing_category:
        raise HTTPException(
            status_code=404,
            detail=f"Категория с ID {category_id} не найдена"
        )
    update_data = category_update.model_dump(exclude_unset=True)
    if not update_data:
        raise HTTPException(
            status_code=400,
            detail="Нет данных для обновления"
        )
    update_query = global_categories.update().where(
        global_categories.c.id == category_id
    ).values(**update_data)
    await database.execute(update_query)
    updated_category_query = select(global_categories).where(
        global_categories.c.id == category_id
    )
    updated_category = await database.fetch_one(updated_category_query)
    updated_category_dict = dict(serialize_datetime_fields(updated_category))
    # Получаем дерево для этой категории
    children_query = select(global_categories).where(
        global_categories.c.parent_id == category_id,
        global_categories.c.is_active.is_(True),
    )
    children = await database.fetch_all(children_query)
    updated_category_dict["children"] = [
        dict(serialize_datetime_fields(child))
        for child in children
    ]
    return updated_category_dict


@router.delete("/{category_id}/")
async def delete_global_category(category_id: int):
    check_query = select(global_categories).where(
        global_categories.c.id == category_id,
        global_categories.c.is_active.is_(True),
    )
    existing_category = await database.fetch_one(check_query)
    if not existing_category:
        raise HTTPException(
            status_code=404,
            detail=f"Категория с ID {category_id} не найдена"
        )
    delete_query = global_categories.update().where(
        global_categories.c.id == category_id
    ).values(is_active=False)
    await database.execute(delete_query)
    return {
        "success": True,
        "message": f"Категория {category_id} успешно удалена"
    }


@router.post("/{category_id}/upload_image/")
async def upload_category_image(
    category_id: int,
    file: UploadFile = File(...)
):
    """
    Загрузка изображения категории

    Принимает изображение и сохраняет его на сервере.
    Поддерживаемые форматы: jpg, jpeg, png, gif, webp
    """
    check_query = select(global_categories).where(
        global_categories.c.id == category_id,
        global_categories.c.is_active.is_(True),
    )
    existing_category = await database.fetch_one(check_query)
    if not existing_category:
        raise HTTPException(
            status_code=404, detail=f"Категория с ID {category_id} не найдена"
        )
    file_extension = Path(file.filename).suffix.lower()
    from .settings import ALLOWED_EXTENSIONS, MAX_UPLOAD_SIZE, UPLOAD_DIR
    if file_extension not in ALLOWED_EXTENSIONS:
        allowed = ', '.join(ALLOWED_EXTENSIONS)
        raise HTTPException(
            status_code=400,
            detail=f"Недопустимый формат файла. Разрешены: {allowed}",
        )
    unique_filename = f"{uuid.uuid4()}{file_extension}"
    file_path = UPLOAD_DIR / unique_filename
    try:
        contents = await file.read()
        if len(contents) > MAX_UPLOAD_SIZE:
            max_mb = MAX_UPLOAD_SIZE / 1024 / 1024
            raise HTTPException(
                status_code=413,
                detail=f"Файл слишком большой. Максимум: {max_mb:.1f}MB",
            )
        with open(file_path, "wb") as f:
            f.write(contents)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Ошибка при сохранении файла: {str(e)}"
        )
    image_url = f"/uploads/categories/{unique_filename}"
    update_query = (
        global_categories.update()
        .where(global_categories.c.id == category_id)
        .values(image_url=image_url)
    )
    await database.execute(update_query)
    return {
        "success": True,
        "image_url": image_url,
        "filename": unique_filename,
        "message": (
            f"Изображение успешно загружено "
            f"для категории {category_id}"
        ),
    }

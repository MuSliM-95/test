"""API endpoints для категорий"""
import uuid
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy import func, select

from ...dependencies import verify_admin_key
from ...config import settings
from ...schemas.category import (
    GlobalCategoryCreate,
    GlobalCategoryList,
    GlobalCategoryTree,
    GlobalCategoryTreeList,
    GlobalCategoryUpdate,
)
from ...utils.helpers import datetime_to_timestamp
from ...database.db import database, global_categories

router = APIRouter(prefix="/global_categories", tags=["categories"])


async def build_global_hierarchy(
    categories_data: List[dict], parent_id: Optional[int] = None
) -> List[dict]:
    """Построение иерархического дерева категорий"""
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
    """Получение списка глобальных категорий"""
    query = (
        select(global_categories)
        .where(global_categories.c.is_active.is_(True))
        .order_by(global_categories.c.name)
        .limit(limit)
        .offset(offset)
    )

    categories_db = await database.fetch_all(query)
    categories_db = [*map(datetime_to_timestamp, categories_db)]

    count_query = select(func.count(global_categories.c.id)).where(
        global_categories.c.is_active.is_(True)
    )
    categories_count = await database.fetch_one(count_query)

    return {"result": categories_db, "count": categories_count.count_1}


@router.get("/tree/", response_model=GlobalCategoryTreeList)
async def get_global_categories_tree():
    """Получение дерева категорий"""
    query = (
        select(global_categories)
        .where(global_categories.c.is_active.is_(True))
        .order_by(global_categories.c.name)
    )

    categories_db = await database.fetch_all(query)
    categories_db = [*map(datetime_to_timestamp, categories_db)]

    # Строим дерево
    tree = await build_global_hierarchy(categories_db, parent_id=None)

    count_query = select(func.count(global_categories.c.id)).where(
        global_categories.c.is_active.is_(True)
    )
    categories_count = await database.fetch_one(count_query)

    return {"result": tree, "count": categories_count.count_1}


@router.get("/{category_id}/", response_model=GlobalCategoryTree)
async def get_global_category_by_id(category_id: int):
    """Получение категории по ID с дочерними элементами"""
    query = select(global_categories).where(
        global_categories.c.id == category_id,
        global_categories.c.is_active.is_(True),
    )

    category_db = await database.fetch_one(query)
    if not category_db:
        raise HTTPException(
            status_code=404, detail=f"Категория с ID {category_id} не найдена"
        )

    category_dict = dict(datetime_to_timestamp(category_db))

    # Получаем дочерние элементы
    children_query = (
        select(global_categories)
        .where(
            global_categories.c.parent_id == category_id,
            global_categories.c.is_active.is_(True),
        )
        .order_by(global_categories.c.name)
    )

    children_db = await database.fetch_all(children_query)
    children_db = [*map(datetime_to_timestamp, children_db)]

    category_dict["children"] = children_db

    return category_dict


@router.post(
    "/", response_model=GlobalCategoryTree, status_code=201
)
async def create_global_category(
    category: GlobalCategoryCreate, _: bool = Depends(verify_admin_key)
):
    """
    Создание новой категории (требуется админский ключ)

    Передайте заголовок: X-Admin-Key: your-secret-admin-key-here
    """
    # Проверяем существование parent_id если он указан
    if category.parent_id:
        parent_query = select(global_categories).where(
            global_categories.c.id == category.parent_id,
            global_categories.c.is_active.is_(True),
        )
        parent_exists = await database.fetch_one(parent_query)
        if not parent_exists:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Родительская категория с ID "
                    f"{category.parent_id} не найдена"
                ),
            )

    # Создаём категорию
    query = global_categories.insert().values(
        name=category.name,
        description=category.description,
        code=category.code,
        parent_id=category.parent_id,
        external_id=category.external_id,
        image_url=category.image_url,
        is_active=category.is_active,
    )

    new_category_id = await database.execute(query)

    # Получаем созданную категорию
    created_category_query = select(global_categories).where(
        global_categories.c.id == new_category_id
    )
    created_category = await database.fetch_one(created_category_query)
    created_category_dict = dict(datetime_to_timestamp(created_category))
    created_category_dict["children"] = []

    return created_category_dict


@router.patch("/{category_id}/", response_model=GlobalCategoryTree)
async def update_global_category(
    category_id: int,
    category_update: GlobalCategoryUpdate,
    _: bool = Depends(verify_admin_key),
):
    """
    Обновление категории (требуется админский ключ)

    Можно обновить только указанные поля, остальные остаются без изменений
    """
    # Проверяем существование категории
    check_query = select(global_categories).where(
        global_categories.c.id == category_id,
        global_categories.c.is_active.is_(True),
    )
    existing_category = await database.fetch_one(check_query)

    if not existing_category:
        raise HTTPException(
            status_code=404, detail=f"Категория с ID {category_id} не найдена"
        )

    # Собираем только не-None поля
    update_data = category_update.model_dump(exclude_unset=True)

    if not update_data:
        raise HTTPException(
            status_code=400,
            detail="Не передано ни одного поля для обновления",
        )

    # Обновляем категорию
    update_query = (
        global_categories.update()
        .where(global_categories.c.id == category_id)
        .values(**update_data)
    )
    await database.execute(update_query)

    # Возвращаем обновлённую категорию
    updated_category = await database.fetch_one(check_query)
    updated_category_dict = dict(datetime_to_timestamp(updated_category))
    updated_category_dict["children"] = []

    return updated_category_dict


@router.delete("/{category_id}/")
async def delete_global_category(
    category_id: int, _: bool = Depends(verify_admin_key)
):
    """
    Удаление (деактивация) категории (требуется админский ключ)

    Категория не удаляется физически, только помечается как неактивная
    """
    # Проверяем существование категории
    check_query = select(global_categories).where(
        global_categories.c.id == category_id,
        global_categories.c.is_active.is_(True),
    )
    existing_category = await database.fetch_one(check_query)

    if not existing_category:
        raise HTTPException(
            status_code=404, detail=f"Категория с ID {category_id} не найдена"
        )

    # Деактивируем категорию (мягкое удаление)
    delete_query = (
        global_categories.update()
        .where(global_categories.c.id == category_id)
        .values(is_active=False)
    )
    await database.execute(delete_query)

    return {
        "success": True,
        "message": f"Категория {category_id} успешно удалена",
    }


@router.post("/{category_id}/upload_image/")
async def upload_category_image(
    category_id: int,
    file: UploadFile = File(...),
    _: bool = Depends(verify_admin_key),
):
    """
    Загрузка изображения категории (требуется админский ключ)

    Принимает изображение и сохраняет его на сервере.
    Поддерживаемые форматы: jpg, jpeg, png, gif, webp
    """
    # Проверяем существование категории
    check_query = select(global_categories).where(
        global_categories.c.id == category_id,
        global_categories.c.is_active.is_(True),
    )
    existing_category = await database.fetch_one(check_query)

    if not existing_category:
        raise HTTPException(
            status_code=404, detail=f"Категория с ID {category_id} не найдена"
        )

    # Проверяем формат файла
    file_extension = Path(file.filename).suffix.lower()

    if file_extension not in settings.ALLOWED_EXTENSIONS:
        allowed = ', '.join(settings.ALLOWED_EXTENSIONS)
        raise HTTPException(
            status_code=400,
            detail=f"Недопустимый формат файла. Разрешены: {allowed}",
        )

    # Создаём уникальное имя файла
    unique_filename = f"{uuid.uuid4()}{file_extension}"

    file_path = settings.UPLOAD_DIR / unique_filename

    # Сохраняем файл
    try:
        contents = await file.read()

        # Проверяем размер файла
        if len(contents) > settings.MAX_UPLOAD_SIZE:
            max_mb = settings.MAX_UPLOAD_SIZE / 1024 / 1024
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

    # Формируем URL для доступа к файлу
    image_url = f"/uploads/categories/{unique_filename}"

    # Обновляем image_url в базе данных
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

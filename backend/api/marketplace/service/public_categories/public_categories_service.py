
from backend.database.db import database, global_categories
from sqlalchemy import select, func
from fastapi import HTTPException, UploadFile
from pathlib import Path
import uuid
from datetime import datetime
from api.marketplace.service.public_categories.schema import (
    GlobalCategoryCreate, GlobalCategoryUpdate
)
from api.marketplace.service.base_marketplace_service import BaseMarketplaceService

UPLOAD_DIR = Path("/uploads/categories")
ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
MAX_UPLOAD_SIZE = 5 * 1024 * 1024


def serialize_datetime_fields(record):
    """
    Преобразует поля created_at и updated_at в isoformat,
    если они есть в record.
    Поддерживает record как dict или sqlalchemy Row.
    """
    result = dict(record)
    for field in ("created_at", "updated_at"):
        value = result.get(field)
        if value is not None and isinstance(value, datetime):
            result[field] = value.isoformat()
    return result


class MarketplacePublicCategoriesService(BaseMarketplaceService):
    async def get_global_categories(self, limit: int = 100, offset: int = 0):
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

    async def build_global_hierarchy(self, categories_data, parent_id=None):
        result = []
        for category in categories_data:
            if category.get("parent_id") == parent_id:
                category_dict = dict(category)
                children = await self.build_global_hierarchy(categories_data, category["id"])
                category_dict["children"] = children
                result.append(category_dict)
        return result

    async def get_global_categories_tree(self):
        query = (
            select(global_categories)
            .where(global_categories.c.is_active.is_(True))
            .order_by(global_categories.c.name)
        )
        categories_db = await database.fetch_all(query)
        categories_db = [*map(serialize_datetime_fields, categories_db)]
        tree = await self.build_global_hierarchy(categories_db, parent_id=None)
        count_query = select(func.count(global_categories.c.id)).where(
            global_categories.c.is_active.is_(True)
        )
        categories_count = await database.fetch_one(count_query)
        return {"result": tree, "count": categories_count.count_1}

    async def get_global_category(self, category_id: int):
        query = select(global_categories).where(
            global_categories.c.id == category_id,
            global_categories.c.is_active.is_(True),
        )
        category = await database.fetch_one(query)
        if not category:
            raise HTTPException(status_code=404, detail="Категория не найдена")
        category_dict = dict(serialize_datetime_fields(category))
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

    async def create_global_category(self, category: GlobalCategoryCreate):
        insert_query = global_categories.insert().values(**category.model_dump())
        new_category_id = await database.execute(insert_query)
        created_category_query = select(global_categories).where(
            global_categories.c.id == new_category_id
        )
        created_category = await database.fetch_one(created_category_query)
        created_category_dict = dict(serialize_datetime_fields(created_category))
        created_category_dict["children"] = []
        return created_category_dict

    async def update_global_category(self, category_id: int, category_update: GlobalCategoryUpdate):
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

    async def delete_global_category(self, category_id: int):
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

    async def upload_category_image(self, category_id: int, file: UploadFile):
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

        UPLOAD_DIR = Path("/uploads/categories")
        ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
        MAX_UPLOAD_SIZE = 5 * 1024 * 1024

        def serialize_datetime_fields(record):
            """
            Преобразует поля created_at и updated_at в isoformat,
            если они есть в record.
            Поддерживает record как dict или sqlalchemy Row.
            """
            result = dict(record)
            for field in ("created_at", "updated_at"):
                value = result.get(field)
                if value is not None and isinstance(value, datetime):
                    result[field] = value.isoformat()
            return result
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

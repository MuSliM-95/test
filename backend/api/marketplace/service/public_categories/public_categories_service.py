import os
import uuid
from datetime import datetime
from pathlib import Path

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.public_categories.schema import (
    GlobalCategoryCreate,
    GlobalCategoryUpdate,
)
from common.s3_service.impl.S3Client import S3Client
from common.s3_service.models.S3SettingsModel import S3SettingsModel
from database.db import (
    database,
    global_categories,
    nomenclature,
    price_types,
    prices,
)
from fastapi import HTTPException, UploadFile
from sqlalchemy import func, select

S3_BUCKET_NAME = "5075293c-docs_generated"
S3_FOLDER = "photos"
S3_URL = os.getenv("S3_URL", "https://s3.yandexcloud.net")
AWS_ACCESS_KEY_ID = os.getenv("S3_ACCESS")
AWS_SECRET_ACCESS_KEY = os.getenv("S3_SECRET")
S3_SETTINGS = S3SettingsModel(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=S3_URL,
)
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__bucket_name = S3_BUCKET_NAME
        self.__s3_client = S3Client(S3_SETTINGS)

    @staticmethod
    async def _get_all_category_ids_recursive(category_id: int) -> list[int]:
        """
        Рекурсивно получает все ID категорий (включая саму категорию и все дочерние).
        """
        all_ids = [category_id]

        # Получаем прямых детей
        children = await database.fetch_all(
            select(global_categories.c.id).where(
                global_categories.c.parent_id == category_id,
                global_categories.c.is_active.is_(True),
            )
        )

        # Рекурсивно получаем детей для каждого ребенка
        for child in children:
            all_ids.extend(
                await MarketplacePublicCategoriesService._get_all_category_ids_recursive(
                    child.id
                )
            )

        return all_ids

    @staticmethod
    async def _check_category_has_products(category_id: int) -> bool:
        """
        Проверяет, есть ли актуальные товары в категории.
        Актуальные товары - это товары с:
        - is_deleted != True
        - price_type = "chatting" (если есть цены)
        - global_category_id = category_id (или в дочерних категориях)

        Если у товара нет цены с price_type="chatting", но есть global_category_id,
        товар все равно считается актуальным для категории.
        """
        from sqlalchemy import and_

        # Получаем все дочерние категории (включая саму категорию)
        all_category_ids = (
            await MarketplacePublicCategoriesService._get_all_category_ids_recursive(
                category_id
            )
        )

        # Если нет категорий для проверки, возвращаем False
        if not all_category_ids:
            return False

        # Сначала проверяем товары с price_type = "chatting" (приоритет)
        try:
            products_with_chatting_query = (
                select(func.count(func.distinct(nomenclature.c.id)))
                .select_from(
                    nomenclature.join(
                        prices, prices.c.nomenclature == nomenclature.c.id
                    ).join(price_types, price_types.c.id == prices.c.price_type)
                )
                .where(
                    and_(
                        nomenclature.c.global_category_id.is_not(None),
                        nomenclature.c.global_category_id.in_(all_category_ids),
                        nomenclature.c.is_deleted.is_not(True),
                        price_types.c.name == "chatting",
                        prices.c.is_deleted.is_not(True),
                    )
                )
            )
            count_with_chatting = await database.fetch_val(products_with_chatting_query)
            if count_with_chatting and count_with_chatting > 0:
                return True
        except Exception:
            pass

        # Если товаров с chatting нет, проверяем любые товары в категории
        # (товары могут быть без цен или с другими типами цен)
        try:
            any_products_query = select(func.count(nomenclature.c.id)).where(
                and_(
                    nomenclature.c.global_category_id.is_not(None),
                    nomenclature.c.global_category_id.in_(all_category_ids),
                    nomenclature.c.is_deleted.is_not(True),
                )
            )
            count_any = await database.fetch_val(any_products_query)
            return count_any > 0 if count_any else False
        except Exception:
            # В случае ошибки возвращаем False, чтобы не ломать вывод категорий
            return False

    async def get_global_categories(
        self, limit: int = 100, offset: int = 0, only_with_products: bool = False
    ):
        from sqlalchemy import and_, exists

        # Базовый запрос категорий
        base_query = select(global_categories).where(
            global_categories.c.is_active.is_(True)
        )

        # Если нужна фильтрация по наличию товаров
        if only_with_products:
            # Подзапрос для проверки наличия товаров в категории
            # (включая товары в дочерних категориях через рекурсивную проверку)
            has_products_subquery = (
                select(1)
                .select_from(
                    nomenclature.join(
                        prices, prices.c.nomenclature == nomenclature.c.id
                    ).join(price_types, price_types.c.id == prices.c.price_type)
                )
                .where(
                    and_(
                        nomenclature.c.global_category_id == global_categories.c.id,
                        nomenclature.c.is_deleted.is_not(True),
                        price_types.c.name == "chatting",
                        prices.c.is_deleted.is_not(True),
                    )
                )
                .limit(1)
            )

            base_query = base_query.where(exists(has_products_subquery))

        query = (
            base_query.order_by(global_categories.c.name).limit(limit).offset(offset)
        )
        categories_db = await database.fetch_all(query)
        categories_db = [*map(serialize_datetime_fields, categories_db)]

        # Используем поле has_products из БД (если есть), иначе вычисляем
        for category in categories_db:
            # Если поле уже есть в БД, используем его
            if "has_products" in category and category["has_products"] is not None:
                # Поле уже загружено из БД
                pass
            else:
                # Вычисляем динамически (fallback для старых данных или если поле не обновлено)
                try:
                    category["has_products"] = await self._check_category_has_products(
                        category["id"]
                    )
                except Exception:
                    category["has_products"] = False

        # Если фильтрация включена, дополнительно фильтруем по has_products
        # (так как SQL запрос проверяет только прямую категорию, а не дочерние)
        if only_with_products:
            categories_db = [
                cat for cat in categories_db if cat.get("has_products", False)
            ]

        # Подсчет общего количества (с учетом фильтрации)
        count_conditions = [global_categories.c.is_active.is_(True)]
        if only_with_products:
            has_products_subquery = (
                select(1)
                .select_from(
                    nomenclature.join(
                        prices, prices.c.nomenclature == nomenclature.c.id
                    ).join(price_types, price_types.c.id == prices.c.price_type)
                )
                .where(
                    and_(
                        nomenclature.c.global_category_id == global_categories.c.id,
                        nomenclature.c.is_deleted.is_not(True),
                        price_types.c.name == "chatting",
                        prices.c.is_deleted.is_not(True),
                    )
                )
                .limit(1)
            )
            count_query = select(func.count(global_categories.c.id)).where(
                and_(*count_conditions), exists(has_products_subquery)
            )
        else:
            count_query = select(func.count(global_categories.c.id)).where(
                and_(*count_conditions)
            )

        categories_count = await database.fetch_one(count_query)
        # Если фильтрация включена, используем реальное количество отфильтрованных
        if only_with_products:
            count = len(categories_db)
        else:
            count = categories_count.count_1

        return {"result": categories_db, "count": count}

    async def build_global_hierarchy(
        self, categories_data, parent_id=None, only_with_products: bool = False
    ):
        result = []
        for category in categories_data:
            if category.get("parent_id") == parent_id:
                category_dict = dict(category)
                children = await self.build_global_hierarchy(
                    categories_data, category["id"], only_with_products
                )
                category_dict["children"] = children

                # Если фильтрация включена, проверяем:
                # - есть ли товары в самой категории (has_products=True)
                # - или есть ли дочерние категории с товарами
                if only_with_products:
                    has_products = category_dict.get("has_products", False)
                    has_children_with_products = len(children) > 0

                    if not has_products and not has_children_with_products:
                        # Пропускаем категории без товаров и без дочерних категорий
                        continue

                result.append(category_dict)
        return result

    async def get_global_categories_tree(self, only_with_products: bool = False):
        # Если фильтрация включена, используем поле has_products из БД
        conditions = [global_categories.c.is_active.is_(True)]
        if only_with_products:
            conditions.append(global_categories.c.has_products.is_(True))

        query = (
            select(global_categories)
            .where(*conditions)
            .order_by(global_categories.c.name)
        )
        categories_db = await database.fetch_all(query)
        categories_db = [*map(serialize_datetime_fields, categories_db)]

        # Используем поле has_products из БД (если есть), иначе вычисляем
        for category in categories_db:
            # Если поле уже есть в БД, используем его
            if "has_products" in category and category["has_products"] is not None:
                # Поле уже загружено из БД
                pass
            else:
                # Вычисляем динамически (fallback для старых данных или если поле не обновлено)
                try:
                    category["has_products"] = await self._check_category_has_products(
                        category["id"]
                    )
                except Exception:
                    category["has_products"] = False

        tree = await self.build_global_hierarchy(
            categories_db, parent_id=None, only_with_products=only_with_products
        )

        # Подсчет с учетом фильтрации
        # Для дерева считаем все категории в дереве (включая вложенные)
        def count_tree_nodes(tree_list):
            count = 0
            for node in tree_list:
                count += 1
                if node.get("children"):
                    count += count_tree_nodes(node["children"])
            return count

        if only_with_products:
            count = count_tree_nodes(tree)
        else:
            count_query = select(func.count(global_categories.c.id)).where(
                global_categories.c.is_active.is_(True)
            )
            count_result = await database.fetch_one(count_query)
            count = count_result.count_1

        return {"result": tree, "count": count}

    async def get_global_category(self, category_id: int):
        query = select(global_categories).where(
            global_categories.c.id == category_id,
            global_categories.c.is_active.is_(True),
        )
        category = await database.fetch_one(query)
        if not category:
            raise HTTPException(status_code=404, detail="Категория не найдена")
        category_dict = dict(serialize_datetime_fields(category))

        # Используем поле has_products из БД (если есть), иначе вычисляем
        if (
            "has_products" in category_dict
            and category_dict["has_products"] is not None
        ):
            # Поле уже загружено из БД
            pass
        else:
            # Вычисляем динамически (fallback для старых данных или если поле не обновлено)
            try:
                category_dict["has_products"] = await self._check_category_has_products(
                    category_id
                )
            except Exception:
                category_dict["has_products"] = False

        children_query = select(global_categories).where(
            global_categories.c.parent_id == category_id,
            global_categories.c.is_active.is_(True),
        )
        children = await database.fetch_all(children_query)
        children_list = [dict(serialize_datetime_fields(child)) for child in children]

        # Используем поле has_products из БД для дочерних категорий
        for child in children_list:
            if "has_products" in child and child["has_products"] is not None:
                # Поле уже загружено из БД
                pass
            else:
                # Вычисляем динамически (fallback для старых данных или если поле не обновлено)
                try:
                    child["has_products"] = await self._check_category_has_products(
                        child["id"]
                    )
                except Exception:
                    child["has_products"] = False

        category_dict["children"] = children_list
        return category_dict

    async def create_global_category(self, category: GlobalCategoryCreate):
        insert_query = global_categories.insert().values(
            **category.dict(exclude={"model_config"})
        )
        new_category_id = await database.execute(insert_query)
        created_category_query = select(global_categories).where(
            global_categories.c.id == new_category_id
        )
        created_category = await database.fetch_one(created_category_query)
        created_category_dict = dict(serialize_datetime_fields(created_category))
        created_category_dict["children"] = []
        return created_category_dict

    async def update_global_category(
        self, category_id: int, category_update: GlobalCategoryUpdate
    ):
        check_query = select(global_categories).where(
            global_categories.c.id == category_id,
        )
        existing_category = await database.fetch_one(check_query)
        if not existing_category:
            raise HTTPException(
                status_code=404, detail=f"Категория с ID {category_id} не найдена"
            )
        update_data = category_update.dict(exclude_unset=True)
        if not update_data:
            raise HTTPException(status_code=400, detail="Нет данных для обновления")
        update_query = (
            global_categories.update()
            .where(global_categories.c.id == category_id)
            .values(**update_data)
        )
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
            dict(serialize_datetime_fields(child)) for child in children
        ]
        return updated_category_dict

    async def delete_global_category(self, category_id: int):
        check_query = select(global_categories).where(
            global_categories.c.id == category_id,
        )
        existing_category = await database.fetch_one(check_query)
        if not existing_category:
            raise HTTPException(
                status_code=404, detail=f"Категория с ID {category_id} не найдена"
            )
        delete_query = (
            global_categories.update()
            .where(global_categories.c.id == category_id)
            .values(is_active=False)
        )
        await database.execute(delete_query)
        return {"success": True, "message": f"Категория {category_id} успешно удалена"}

    async def upload_category_image(self, category_id: int, file: UploadFile):
        check_query = select(global_categories).where(
            global_categories.c.id == category_id,
        )
        existing_category = await database.fetch_one(check_query)
        if not existing_category:
            raise HTTPException(
                status_code=404, detail=f"Категория с ID {category_id} не найдена"
            )
        file_extension = Path(file.filename).suffix.lower()

        if file_extension not in ALLOWED_EXTENSIONS:
            allowed = ", ".join(ALLOWED_EXTENSIONS)
            raise HTTPException(
                status_code=400,
                detail=f"Недопустимый формат файла. Разрешены: {allowed}",
            )
        unique_filename = f"{uuid.uuid4()}{file_extension}"
        s3_key = f"{S3_FOLDER}/{unique_filename}"
        try:
            contents = await file.read()
            if len(contents) > MAX_UPLOAD_SIZE:
                max_mb = MAX_UPLOAD_SIZE / 1024 / 1024
                raise HTTPException(
                    status_code=413,
                    detail=f"Файл слишком большой. Максимум: {max_mb:.1f}MB",
                )
            await self.__s3_client.upload_file_object(
                self.__bucket_name, s3_key, contents
            )
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Ошибка при загрузке файла в S3: {str(e)}"
            )
        # Сохраняем только ключ (key) файла, а не полный URL
        update_query = (
            global_categories.update()
            .where(global_categories.c.id == category_id)
            .values(image_url=s3_key)
        )
        await database.execute(update_query)
        return {
            "success": True,
            "image_key": s3_key,
            "filename": unique_filename,
            "message": (f"Изображение успешно загружено для категории {category_id}"),
        }

    async def ensure_global_category_exists(self, category_id: int) -> None:
        query = select(global_categories.c.id).where(
            global_categories.c.id == category_id,
            global_categories.c.is_active.is_(True),
        )
        category = await database.fetch_one(query)
        if not category:
            raise HTTPException(status_code=404, detail="Категория не найдена")

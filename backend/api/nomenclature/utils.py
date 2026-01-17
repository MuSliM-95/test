"""
Утилиты для работы с номенклатурой
"""

from typing import List, Optional

from sqlalchemy import and_, func, select

from api.marketplace.service.public_categories.public_categories_service import (
    MarketplacePublicCategoriesService,
)
from database.db import (
    categories,
    database,
    global_categories,
    nomenclature,
    price_types,
    prices,
)


async def auto_link_global_category(
    local_category_id: Optional[int],
    nomenclature_id: Optional[int] = None,
) -> Optional[int]:
    """
    Автоматически связывает товар с глобальной категорией на основе локальной категории.

    Логика:
    1. Если у товара есть локальная категория, ищем глобальную категорию с таким же именем
    2. Если не найдено, ищем по частичному совпадению имени
    3. Если не найдено, возвращаем None

    Args:
        local_category_id: ID локальной категории товара
        nomenclature_id: ID товара (опционально, для логирования)

    Returns:
        ID глобальной категории или None
    """
    if not local_category_id:
        return None

    # Получаем локальную категорию
    local_cat_query = select(categories.c.name).where(
        categories.c.id == local_category_id, categories.c.is_deleted.is_not(True)
    )
    local_cat = await database.fetch_one(local_cat_query)

    if not local_cat:
        return None

    local_cat_name = local_cat.name.strip()

    # Ищем глобальную категорию с точным совпадением имени
    global_cat_query = (
        select(global_categories.c.id)
        .where(
            func.trim(global_categories.c.name) == local_cat_name,
            global_categories.c.is_active.is_(True),
        )
        .limit(1)
    )

    global_cat = await database.fetch_one(global_cat_query)

    if global_cat:
        return global_cat.id

    # Если точного совпадения нет, ищем по частичному совпадению (без учета регистра)
    global_cat_query = (
        select(global_categories.c.id)
        .where(
            func.lower(func.trim(global_categories.c.name))
            == func.lower(local_cat_name),
            global_categories.c.is_active.is_(True),
        )
        .limit(1)
    )

    global_cat = await database.fetch_one(global_cat_query)

    if global_cat:
        return global_cat.id

    # Если не найдено, возвращаем None
    return None


async def sync_global_category_for_nomenclature(
    nomenclature_id: int,
    local_category_id: Optional[int] = None,
) -> Optional[int]:
    """
    Синхронизирует global_category_id для товара на основе локальной категории.
    Вызывается при создании/обновлении товара.

    Args:
        nomenclature_id: ID товара
        local_category_id: ID локальной категории (если не указан, берется из товара)

    Returns:
        ID глобальной категории или None
    """
    # Если local_category_id не указан, получаем его из товара
    if local_category_id is None:
        nom_query = select(nomenclature.c.category).where(
            nomenclature.c.id == nomenclature_id
        )
        nom = await database.fetch_one(nom_query)
        if not nom or not nom.category:
            return None
        local_category_id = nom.category

    # Автоматически связываем с глобальной категорией
    global_category_id = await auto_link_global_category(
        local_category_id, nomenclature_id
    )

    # Обновляем товар, если нашли глобальную категорию
    if global_category_id:
        update_query = (
            nomenclature.update()
            .where(nomenclature.c.id == nomenclature_id)
            .values(global_category_id=global_category_id)
        )
        await database.execute(update_query)
        return global_category_id

    return None


async def update_category_has_products(category_id: int) -> None:
    """
    Обновляет поле has_products для категории и всех родительских категорий.
    Проверяет наличие актуальных товаров в категории и её дочерних категориях.

    Args:
        category_id: ID глобальной категории
    """
    # Получаем все дочерние категории (включая саму категорию)
    all_category_ids = (
        await MarketplacePublicCategoriesService._get_all_category_ids_recursive(
            category_id
        )
    )

    if not all_category_ids:
        # Если нет категорий, устанавливаем has_products = False
        await database.execute(
            "UPDATE global_categories SET has_products = FALSE WHERE id = :cat_id",
            values={"cat_id": category_id},
        )
        return

    # Проверяем наличие товаров с price_type = "chatting" (приоритет)
    chatting_count = await database.fetch_val(
        select(func.count(func.distinct(nomenclature.c.id)))
        .select_from(
            nomenclature.join(prices, prices.c.nomenclature == nomenclature.c.id).join(
                price_types, price_types.c.id == prices.c.price_type
            )
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

    if chatting_count and chatting_count > 0:
        has_products = True
    else:
        # Если товаров с chatting нет, проверяем любые товары
        any_count = await database.fetch_val(
            select(func.count(nomenclature.c.id)).where(
                and_(
                    nomenclature.c.global_category_id.is_not(None),
                    nomenclature.c.global_category_id.in_(all_category_ids),
                    nomenclature.c.is_deleted.is_not(True),
                )
            )
        )
        has_products = any_count > 0 if any_count else False

    # Обновляем has_products для категории
    await database.execute(
        "UPDATE global_categories SET has_products = :has_products WHERE id = :cat_id",
        values={"has_products": has_products, "cat_id": category_id},
    )

    # Обновляем все родительские категории
    # Получаем parent_id текущей категории
    parent = await database.fetch_one(
        "SELECT parent_id FROM global_categories WHERE id = :cat_id",
        values={"cat_id": category_id},
    )

    if parent and parent.parent_id:
        # Рекурсивно обновляем родительскую категорию
        await update_category_has_products(parent.parent_id)


async def update_categories_has_products(category_ids: List[int]) -> None:
    """
    Обновляет has_products для нескольких категорий.

    Args:
        category_ids: Список ID глобальных категорий
    """
    for category_id in category_ids:
        if category_id:
            await update_category_has_products(category_id)


async def update_category_has_products(category_id: int) -> None:
    """
    Обновляет поле has_products для категории и всех родительских категорий.
    Проверяет наличие актуальных товаров в категории и её дочерних категориях.

    Args:
        category_id: ID глобальной категории
    """
    # Получаем все дочерние категории (включая саму категорию)
    all_category_ids = (
        await MarketplacePublicCategoriesService._get_all_category_ids_recursive(
            category_id
        )
    )

    if not all_category_ids:
        # Если нет категорий, устанавливаем has_products = False
        await database.execute(
            "UPDATE global_categories SET has_products = FALSE WHERE id = :cat_id",
            values={"cat_id": category_id},
        )
        return

    # Проверяем наличие товаров с price_type = "chatting" (приоритет)
    chatting_count = await database.fetch_val(
        select(func.count(func.distinct(nomenclature.c.id)))
        .select_from(
            nomenclature.join(prices, prices.c.nomenclature == nomenclature.c.id).join(
                price_types, price_types.c.id == prices.c.price_type
            )
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

    if chatting_count and chatting_count > 0:
        has_products = True
    else:
        # Если товаров с chatting нет, проверяем любые товары
        any_count = await database.fetch_val(
            select(func.count(nomenclature.c.id)).where(
                and_(
                    nomenclature.c.global_category_id.is_not(None),
                    nomenclature.c.global_category_id.in_(all_category_ids),
                    nomenclature.c.is_deleted.is_not(True),
                )
            )
        )
        has_products = any_count > 0 if any_count else False

    # Обновляем has_products для категории
    await database.execute(
        "UPDATE global_categories SET has_products = :has_products WHERE id = :cat_id",
        values={"has_products": has_products, "cat_id": category_id},
    )

    # Обновляем все родительские категории
    # Получаем parent_id текущей категории
    parent = await database.fetch_one(
        "SELECT parent_id FROM global_categories WHERE id = :cat_id",
        values={"cat_id": category_id},
    )

    if parent and parent.parent_id:
        # Рекурсивно обновляем родительскую категорию
        await update_category_has_products(parent.parent_id)


async def update_categories_has_products(category_ids: List[int]) -> None:
    """
    Обновляет has_products для нескольких категорий.

    Args:
        category_ids: Список ID глобальных категорий
    """
    for category_id in category_ids:
        if category_id:
            await update_category_has_products(category_id)

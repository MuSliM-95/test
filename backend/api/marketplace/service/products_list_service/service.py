import json
from datetime import datetime
from typing import List, Optional

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.products_list_service.schemas import (
    AvailableWarehouse,
    MarketplaceProduct,
    MarketplaceProductAttribute,
    MarketplaceProductDetail,
    MarketplaceProductList,
    MarketplaceProductsRequest,
    MarketplaceSort,
    product_buttons_text,
)
from api.marketplace.service.public_categories.public_categories_service import (
    MarketplacePublicCategoriesService,
)
from common.utils.url_helper import get_app_url_for_environment
from database.db import (
    categories,
    cboxes,
    database,
    docs_sales_goods,
    manufacturers,
    marketplace_rating_aggregates,
    nomenclature,
    nomenclature_attributes,
    nomenclature_attributes_value,
    nomenclature_barcodes,
    nomenclature_groups,
    nomenclature_groups_value,
    pictures,
    price_types,
    prices,
    units,
    users,
    warehouse_balances,
    warehouses,
)
from fastapi import HTTPException
from sqlalchemy import and_, asc, desc, func, literal_column, select


class MarketplaceProductsListService(BaseMarketplaceService):
    @staticmethod
    def __transform_photo_route(photo_path: Optional[str]) -> Optional[str]:
        if not photo_path:
            return None
        base_url = get_app_url_for_environment()
        if not base_url:
            raise ValueError("APP_URL не настроен для текущего окружения")
        photo_url = photo_path.lstrip("/")

        if "seller" in photo_url:
            return f"https://{base_url}/api/v1/{photo_path.lstrip('/')}"
        else:
            return f"https://{base_url}/{photo_path.lstrip('/')}"

    @staticmethod
    def __build_picture_url(picture_id: Optional[int]) -> Optional[str]:
        """Строит публичный URL для фото по ID"""
        if not picture_id:
            return None
        base_url = get_app_url_for_environment()
        if not base_url:
            return None
        # Добавляем протокол, если его нет
        if not base_url.startswith(("http://", "https://")):
            base_url = f"https://{base_url}"
        return f"{base_url}/api/v1/pictures/{picture_id}/content"

    async def get_product(self, product_id: int) -> MarketplaceProductDetail:
        current_timestamp = int(datetime.now().timestamp())

        ranked_prices_subquery = (
            select(
                prices.c.nomenclature.label("nomenclature_id"),
                prices.c.id.label("price_id"),
                prices.c.price,
                prices.c.price_type,
                prices.c.created_at,
                prices.c.date_from,
                prices.c.date_to,
                prices.c.is_deleted,
                func.row_number()
                .over(
                    partition_by=prices.c.nomenclature,
                    order_by=[
                        desc(
                            func.coalesce(prices.c.date_from <= current_timestamp, True)
                            & func.coalesce(current_timestamp < prices.c.date_to, True)
                        ),
                        desc(prices.c.created_at),
                        desc(prices.c.id),
                    ],
                )
                .label("rn"),
            )
            .select_from(
                prices.join(price_types, price_types.c.id == prices.c.price_type)
            )
            .where(
                prices.c.is_deleted.is_not(True),
                price_types.c.name == "chatting",
            )
            .subquery()
        )

        active_prices_subquery = (
            select(ranked_prices_subquery)
            .where(ranked_prices_subquery.c.rn == 1)
            .subquery()
        )

        total_sold_subquery = (
            select(
                docs_sales_goods.c.nomenclature,
                func.count(docs_sales_goods.c.id).label("total_sold"),
            )
            .group_by(docs_sales_goods.c.nomenclature)
            .subquery()
        )

        # Основной запрос для получения базовой информации о товаре
        query = (
            select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.description_short,
                nomenclature.c.description_long,
                nomenclature.c.code,
                nomenclature.c.cashbox,
                nomenclature.c.created_at,
                nomenclature.c.updated_at,
                nomenclature.c.tags,
                nomenclature.c.type,
                nomenclature.c.seo_title,
                nomenclature.c.seo_description,
                nomenclature.c.seo_keywords,
                nomenclature.c.production_time_min_from,
                nomenclature.c.production_time_min_to,
                units.c.convent_national_view.label("unit_name"),
                categories.c.name.label("category_name"),
                manufacturers.c.name.label("manufacturer_name"),
                active_prices_subquery.c.price,
                price_types.c.name.label("price_type"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_name, ""),
                    cboxes.c.name,
                ).label("seller_name"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_photo, ""),
                    users.c.photo,
                ).label("seller_photo"),
                cboxes.c.seller_description.label("seller_description"),
                marketplace_rating_aggregates.c.avg_rating.label("rating"),
                marketplace_rating_aggregates.c.reviews_count.label("reviews_count"),
                func.array_agg(func.distinct(pictures.c.id))
                .filter(pictures.c.id.is_not(None))
                .label("images"),
                func.array_agg(func.distinct(nomenclature_barcodes.c.code))
                .filter(nomenclature_barcodes.c.code.is_not(None))
                .label("barcodes"),
                func.coalesce(total_sold_subquery.c.total_sold, 0).label("total_sold"),
            )
            .select_from(nomenclature)
            .join(units, units.c.id == nomenclature.c.unit, isouter=True)
            .join(categories, categories.c.id == nomenclature.c.category, isouter=True)
            .join(
                manufacturers,
                manufacturers.c.id == nomenclature.c.manufacturer,
                isouter=True,
            )
            .join(
                active_prices_subquery,
                active_prices_subquery.c.nomenclature_id == nomenclature.c.id,
            )
            .join(price_types, price_types.c.id == active_prices_subquery.c.price_type)
            .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
            .join(users, users.c.id == cboxes.c.admin, isouter=True)
            .join(
                pictures,
                and_(
                    pictures.c.entity == "nomenclature",
                    pictures.c.entity_id == nomenclature.c.id,
                    pictures.c.is_deleted.is_not(True),
                ),
                isouter=True,
            )
            .join(
                nomenclature_barcodes,
                nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )
            .join(
                marketplace_rating_aggregates,
                and_(
                    marketplace_rating_aggregates.c.entity_id == nomenclature.c.id,
                    marketplace_rating_aggregates.c.entity_type == "nomenclature",
                ),
                isouter=True,
            )
            .join(
                total_sold_subquery,
                total_sold_subquery.c.nomenclature == nomenclature.c.id,
                isouter=True,
            )
            .where(
                and_(
                    nomenclature.c.id == product_id,
                    nomenclature.c.is_deleted.is_not(True),
                    price_types.c.name == "chatting",
                )
            )
            .group_by(
                nomenclature.c.id,
                units.c.convent_national_view,
                categories.c.name,
                manufacturers.c.name,
                active_prices_subquery.c.price,
                price_types.c.name,
                cboxes.c.seller_name,
                cboxes.c.name,
                cboxes.c.seller_photo,
                users.c.photo,
                cboxes.c.seller_description,
                marketplace_rating_aggregates.c.avg_rating,
                marketplace_rating_aggregates.c.reviews_count,
                total_sold_subquery.c.total_sold,
            )
        )

        row = await database.fetch_one(query)
        if not row:
            raise HTTPException(status_code=404, detail="Товар не найден")

        product = dict(row)

        # Отдельный запрос для получения складов с остатками
        # ОПТИМИЗАЦИЯ: Используем подзапрос с MAX вместо прямого запроса - получаем только последние остатки
        wb_max_for_product = (
            select(
                warehouse_balances.c.organization_id,
                warehouse_balances.c.warehouse_id,
                warehouse_balances.c.nomenclature_id,
                func.max(warehouse_balances.c.id).label("max_id"),
            )
            .where(warehouse_balances.c.nomenclature_id == product_id)
            .group_by(
                warehouse_balances.c.organization_id,
                warehouse_balances.c.warehouse_id,
                warehouse_balances.c.nomenclature_id,
            )
            .subquery()
        )

        warehouses_query = (
            select(
                warehouses.c.id.label("warehouse_id"),
                warehouses.c.name.label("warehouse_name"),
                warehouses.c.address.label("warehouse_address"),
                warehouses.c.latitude,
                warehouses.c.longitude,
                warehouse_balances.c.current_amount,
                warehouse_balances.c.organization_id,
            )
            .select_from(
                warehouse_balances.join(
                    wb_max_for_product,
                    and_(
                        warehouse_balances.c.organization_id
                        == wb_max_for_product.c.organization_id,
                        warehouse_balances.c.warehouse_id
                        == wb_max_for_product.c.warehouse_id,
                        warehouse_balances.c.nomenclature_id
                        == wb_max_for_product.c.nomenclature_id,
                        warehouse_balances.c.id == wb_max_for_product.c.max_id,
                    ),
                ).join(
                    warehouses,
                    and_(
                        warehouses.c.id == warehouse_balances.c.warehouse_id,
                        warehouses.c.is_deleted.is_not(True),
                    ),
                )
            )
            .where(
                and_(
                    warehouse_balances.c.nomenclature_id == product_id,
                    warehouse_balances.c.current_amount
                    > 0,  # Только склады с остатками
                )
            )
        )

        warehouses_rows = await database.fetch_all(warehouses_query)

        total_amount = 0
        available_warehouses = []
        for wh_row in warehouses_rows:
            wh_dict = dict(wh_row)
            total_amount += wh_dict["current_amount"] or 0
            available_warehouses.append(
                AvailableWarehouse(
                    warehouse_id=wh_dict["warehouse_id"],
                    organization_id=wh_dict["organization_id"],
                    warehouse_name=wh_dict["warehouse_name"],
                    warehouse_address=wh_dict["warehouse_address"],
                    latitude=wh_dict["latitude"],
                    longitude=wh_dict["longitude"],
                    current_amount=wh_dict["current_amount"],
                    distance_to_client=self._count_distance_to_client(
                        None, None, wh_dict["latitude"], wh_dict["longitude"]
                    ),
                )
            )

        product["available_warehouses"] = available_warehouses or None
        product["current_amount"] = total_amount

        # Вариации товаров
        # 1. Получаем все группы, к которым принадлежит товар
        group_query = (
            select(
                nomenclature_groups_value.c.group_id,
                nomenclature_groups.c.name.label("group_name"),
            )
            .select_from(nomenclature_groups_value)
            .join(
                nomenclature_groups,
                nomenclature_groups.c.id == nomenclature_groups_value.c.group_id,
            )
            .where(nomenclature_groups_value.c.nomenclature_id == product_id)
        )

        groups = await database.fetch_all(group_query)

        nomenclatures_result = []

        # Получаем все вариации одним запросом вместо множества запросов (избегаем N+1)
        variations_by_group = {}
        if groups:
            group_ids = [group["group_id"] for group in groups]

            # Загружаем все вариации для всех групп сразу
            all_variations_query = (
                select(
                    nomenclature_groups_value.c.group_id,
                    nomenclature.c.id,
                    nomenclature.c.name,
                    nomenclature_groups_value.c.is_main,
                )
                .select_from(nomenclature_groups_value)
                .join(
                    nomenclature,
                    nomenclature.c.id == nomenclature_groups_value.c.nomenclature_id,
                )
                .where(nomenclature_groups_value.c.group_id.in_(group_ids))
            )
            all_variations = await database.fetch_all(all_variations_query)

            # Раскладываем вариации по группам
            for variation in all_variations:
                group_id = variation["group_id"]
                if group_id not in variations_by_group:
                    variations_by_group[group_id] = []
                variations_by_group[group_id].append(
                    {
                        "id": variation["id"],
                        "name": variation["name"],
                        "is_main": variation["is_main"],
                    }
                )

        # Используем уже загруженные вариации для каждой группы
        for group in groups:
            group_id = group["group_id"]
            group_name = group["group_name"]

            variations = variations_by_group.get(group_id, [])

            items = [
                {"id": v["id"], "name": v["name"], "is_main": v["is_main"]}
                for v in variations
            ]

            nomenclatures_result.append({"group_name": group_name, "items": items})

        product["nomenclatures"] = nomenclatures_result or None

        # Фото - преобразуем ID фото в публичные URL
        if (
            product.get("images")
            and isinstance(product["images"], list)
            and any(product["images"])
        ):
            product["images"] = [
                self.__build_picture_url(picture_id)
                for picture_id in product["images"]
                if picture_id is not None
            ]
            # Убираем None значения
            product["images"] = [
                url for url in product["images"] if url is not None
            ] or None

        # Селлер
        if product["seller_photo"]:
            product["seller_photo"] = self.__transform_photo_route(
                product["seller_photo"]
            )

        # Штрихкоды
        product["barcodes"] = [b for b in (product["barcodes"] or []) if b]

        # Поле cashbox_id
        product["cashbox_id"] = product["cashbox"]

        product["listing_pos"] = 1
        product["listing_page"] = 1
        product["is_ad_pos"] = False
        product["variations"] = []

        # distance — расстояние до ближайшего склада
        if product["available_warehouses"]:
            product["distance"] = min(
                product["available_warehouses"],
                key=lambda x: (x.distance_to_client is None, x.distance_to_client or 0),
            ).distance_to_client
        else:
            product["distance"] = None

        # Добавляем атрибуты
        attrs = await database.fetch_all(
            select(
                nomenclature_attributes.c.name, nomenclature_attributes_value.c.value
            )
            .select_from(nomenclature_attributes_value)
            .join(
                nomenclature_attributes,
                nomenclature_attributes.c.id
                == nomenclature_attributes_value.c.attribute_id,
            )
            .where(nomenclature_attributes_value.c.nomenclature_id == product_id)
        )
        product_attributes = [
            MarketplaceProductAttribute(name=a.name, value=a.value) for a in attrs
        ]

        return MarketplaceProductDetail(**product, attributes=product_attributes)

    async def get_products(
        self,
        request: MarketplaceProductsRequest,
    ) -> MarketplaceProductList:
        # --- НАЧАЛО: Подзапрос для выбора актуальной цены ---
        # Получаем текущий timestamp (в PostgreSQL это NOW())
        current_timestamp = int(datetime.now().timestamp())

        # Подзапрос для получения актуальной цены
        # Используем ROW_NUMBER для ранжирования цен по приоритету
        ranked_prices_subquery = (
            select(
                prices.c.nomenclature.label("nomenclature_id"),
                prices.c.id.label("price_id"),
                prices.c.price,
                prices.c.price_type,
                prices.c.created_at,
                prices.c.date_from,
                prices.c.date_to,
                prices.c.is_deleted,
                # Ранжируем: 1. по времени (текущее в интервале), 2. по дате создания (новые первые), 3. по id (стабильность)
                func.row_number()
                .over(
                    partition_by=prices.c.nomenclature,
                    order_by=[
                        # Если дата не указана (None), считаем, что она подходит (предполагаем, что None означает "без ограничений")
                        # Сначала идут записи с датами, которые соответствуют условию
                        # func.coalesce(prices.c.date_from <= current_timestamp, True) & func.coalesce(current_timestamp < prices.c.date_to, True), # Не подходит напрямую для ORDER BY
                        # Правильный способ - использовать CASE/WHEN для приоритета
                        # Приоритет 1: цена с указанными датами и попадающая в интервал
                        # Приоритет 2: цена с указанными датами, но НЕ попадающая в интервал (меньше приоритет)
                        # Приоритет 3: цена с хотя бы одной датой None (предполагаем, что это "постоянно действительна", но может быть иначе)
                        # Пример: сначала (1, ...), потом (0, 1, ...), потом (0, 0, ...)
                        # (CASE WHEN (prices.c.date_from IS NOT NULL AND prices.c.date_to IS NOT NULL AND prices.c.date_from <= current_timestamp AND current_timestamp < prices.c.date_to) THEN 1 ELSE 0 END),
                        # desc(CASE WHEN (prices.c.date_from IS NOT NULL AND prices.c.date_to IS NOT NULL AND prices.c.date_from <= current_timestamp AND current_timestamp < prices.c.date_to) THEN 1 ELSE 0 END),
                        # desc((prices.c.date_from <= current_timestamp) & (current_timestamp < prices.c.date_to)),
                        # Сортировка: сначала те, у кого даты указаны и они подходят, потом без дат (или частично), потом по created_at DESC
                        # Чтобы сначала шли подходящие по времени:
                        desc(
                            func.coalesce(prices.c.date_from <= current_timestamp, True)
                            & func.coalesce(current_timestamp < prices.c.date_to, True)
                        ),
                        # Потом по дате создания (новые первые)
                        desc(prices.c.created_at),
                        # Потом по ID (стабильность)
                        desc(prices.c.id),
                    ],
                )
                .label("rn"),
            )
            .select_from(
                prices.join(price_types, price_types.c.id == prices.c.price_type)
            )
            .where(
                # Учитываем только неудаленные цены
                prices.c.is_deleted.is_not(True),
                # Берём только цены типа "chatting"
                price_types.c.name == "chatting",
            )
            .subquery()
        )

        # Фильтруем подзапрос, чтобы оставить только первую (самую приоритетную) цену для каждой номенклатуры
        # Это будет "актуальная" цена
        active_prices_subquery = (
            select(ranked_prices_subquery)
            .where(ranked_prices_subquery.c.rn == 1)
            .subquery()
        )

        # Считаем количество продаж только для товаров, у которых есть цены
        # Это быстрее, чем считать для всех товаров (оптимизация для docs_sales_goods с 1+ млн строк)
        nomenclature_with_prices = (
            select(active_prices_subquery.c.nomenclature_id).distinct().subquery()
        )

        total_sold_subquery = (
            select(
                docs_sales_goods.c.nomenclature,
                func.count(docs_sales_goods.c.id).label("total_sold"),
            )
            .where(
                docs_sales_goods.c.nomenclature.in_(
                    select(nomenclature_with_prices.c.nomenclature_id)
                )
            )
            .group_by(docs_sales_goods.c.nomenclature)
            .subquery()
        )

        # --- НАЧАЛО: Подзапрос для поиска по атрибутам ---
        # Создаём подзапрос только если есть значение для поиска по атрибутам
        attrs_subquery = None
        if request.nomenclature_attributes:
            attrs_subquery = (
                select(nomenclature_attributes_value.c.nomenclature_id)
                .select_from(
                    nomenclature_attributes_value.join(
                        nomenclature_attributes,
                        nomenclature_attributes.c.id
                        == nomenclature_attributes_value.c.attribute_id,
                    )
                )
                .where(
                    func.lower(nomenclature_attributes_value.c.value).ilike(
                        f"%{request.nomenclature_attributes.lower()}%"
                    )
                )
                .distinct()
                .subquery()
            )
        # --- КОНЕЦ: Подзапрос для поиска по атрибутам ---

        # --- Балансы по складам ---

        # 1) Берём последнюю запись по каждой паре (организация, склад, номенклатура)
        wb_ranked = select(
            warehouse_balances.c.organization_id.label("organization_id"),
            warehouse_balances.c.warehouse_id.label("warehouse_id"),
            warehouse_balances.c.nomenclature_id.label("nomenclature_id"),
            warehouse_balances.c.current_amount.label("current_amount"),
            func.row_number()
            .over(
                partition_by=[
                    warehouse_balances.c.organization_id,
                    warehouse_balances.c.warehouse_id,
                    warehouse_balances.c.nomenclature_id,
                ],
                order_by=[
                    desc(warehouse_balances.c.created_at),
                    desc(warehouse_balances.c.id),
                ],
            )
            .label("rn"),
        ).subquery()

        wb_latest = (
            select(
                wb_ranked.c.organization_id,
                wb_ranked.c.warehouse_id,
                wb_ranked.c.nomenclature_id,
                wb_ranked.c.current_amount,
            )
            .where(wb_ranked.c.rn == 1)
            .subquery()
        )

        # 2) Подсчёт суммарного остатка по товару (только положительные остатки)
        stock_subquery = (
            select(
                wb_latest.c.nomenclature_id.label("nomenclature_id"),
                func.sum(func.greatest(wb_latest.c.current_amount, 0)).label(
                    "current_amount"
                ),
            )
            .select_from(wb_latest)
            .group_by(wb_latest.c.nomenclature_id)
            .subquery()
        )

        # 3) Подготовка алиаса для складов (используется в отдельном запросе для складов)
        wh_bal = warehouses.alias("wh_bal")

        # 4) Подзапрос для определения, есть ли у товара склад в указанном городе
        city_warehouse_subquery = None
        if request.city:
            # Проверяем, есть ли у товара склад с адресом, содержащим указанный город
            city_warehouse_subquery = (
                select(
                    wb_latest.c.nomenclature_id.label("nomenclature_id"),
                    func.bool_or(
                        func.lower(wh_bal.c.address).ilike(f"%{request.city.lower()}%")
                    ).label("has_city_warehouse"),
                )
                .select_from(
                    wb_latest.join(
                        wh_bal,
                        and_(
                            wh_bal.c.id == wb_latest.c.warehouse_id,
                            wh_bal.c.is_deleted.is_not(True),
                        ),
                    )
                )
                .where(wh_bal.c.address.is_not(None))
                .group_by(wb_latest.c.nomenclature_id)
                .subquery()
            )

        # Приоритетная сортировка будет выполнена в Python после получения данных
        # Это избегает проблем с типами данных в SQL

        # --- Основной запрос по товарам ---
        # Формируем список колонок для select
        select_columns = [
            nomenclature.c.id,
            nomenclature.c.name,
            nomenclature.c.description_short,
            nomenclature.c.description_long,
            nomenclature.c.code,
            nomenclature.c.cashbox,
            nomenclature.c.created_at,
            nomenclature.c.updated_at,
            nomenclature.c.tags,
            nomenclature.c.type,
            nomenclature.c.production_time_min_from,
            nomenclature.c.production_time_min_to,
            units.c.convent_national_view.label("unit_name"),
            categories.c.name.label("category_name"),
            manufacturers.c.name.label("manufacturer_name"),
            active_prices_subquery.c.price,
            price_types.c.name.label("price_type"),
            func.coalesce(
                func.nullif(cboxes.c.seller_name, ""),
                cboxes.c.name,
            ).label("seller_name"),
            func.coalesce(
                func.nullif(cboxes.c.seller_photo, ""),
                users.c.photo,
            ).label("seller_photo"),
            cboxes.c.seller_description.label("seller_description"),
            marketplace_rating_aggregates.c.avg_rating.label("rating"),
            marketplace_rating_aggregates.c.reviews_count.label("reviews_count"),
            func.array_agg(func.distinct(pictures.c.id))
            .filter(pictures.c.id.is_not(None))
            .label("images"),
            func.array_agg(func.distinct(nomenclature_barcodes.c.code))
            .filter(nomenclature_barcodes.c.code.is_not(None))
            .label("barcodes"),
            # суммарный остаток по всем складам (минимум 0)
            func.coalesce(stock_subquery.c.current_amount, 0).label("current_amount"),
            func.coalesce(total_sold_subquery.c.total_sold, 0).label("total_sold"),
            # Склады получаем отдельным запросом после основного - это быстрее
            literal_column("NULL::jsonb[]").label("available_warehouses"),
            # Добавляем cashbox_id для определения приоритета в Python
            nomenclature.c.cashbox.label("cashbox_id"),
        ]

        query = (
            select(*select_columns)
            .select_from(nomenclature)
            .join(units, units.c.id == nomenclature.c.unit, isouter=True)
            .join(categories, categories.c.id == nomenclature.c.category, isouter=True)
            .join(
                manufacturers,
                manufacturers.c.id == nomenclature.c.manufacturer,
                isouter=True,
            )
            .join(
                active_prices_subquery,
                active_prices_subquery.c.nomenclature_id == nomenclature.c.id,
            )
            .join(
                price_types,
                price_types.c.id == active_prices_subquery.c.price_type,
            )
            .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
            .join(users, users.c.id == cboxes.c.admin, isouter=True)
            .join(
                pictures,
                and_(
                    pictures.c.entity == "nomenclature",
                    pictures.c.entity_id == nomenclature.c.id,
                    pictures.c.is_deleted.is_not(True),
                ),
                isouter=True,
            )
            .join(
                nomenclature_barcodes,
                nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )
            .join(
                marketplace_rating_aggregates,
                and_(
                    marketplace_rating_aggregates.c.entity_id == nomenclature.c.id,
                    marketplace_rating_aggregates.c.entity_type == "nomenclature",
                ),
                isouter=True,
            )
            .join(
                stock_subquery,
                stock_subquery.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )
            .join(
                total_sold_subquery,
                total_sold_subquery.c.nomenclature == nomenclature.c.id,
                isouter=True,
            )
        )

        # JOIN с подзапросом города не нужен в основном запросе
        # Будем определять город по складам после получения данных

        # Не соединяем со складами в основном запросе - получаем их отдельно
        # Это быстрее и избегает дублирования строк

        if request.nomenclature_attributes:
            query = query.join(
                attrs_subquery,
                attrs_subquery.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )

        # --- Условия фильтрации ---
        conditions = [
            nomenclature.c.is_deleted.is_not(True),
            price_types.c.name == "chatting",
        ]

        if request.category:
            conditions.append(categories.c.name.ilike(f"%{request.category}%"))
        if request.manufacturer:
            conditions.append(manufacturers.c.name.ilike(f"%{request.manufacturer}%"))
        if request.min_price is not None:
            conditions.append(active_prices_subquery.c.price >= request.min_price)
        if request.max_price is not None:
            conditions.append(active_prices_subquery.c.price <= request.max_price)
        if request.in_stock:
            # фильтруем по суммарному остатку
            conditions.append(stock_subquery.c.current_amount > 0)
        if request.rating_from:
            conditions.append(
                marketplace_rating_aggregates.c.avg_rating >= request.rating_from
            )
        if request.rating_to:
            conditions.append(
                marketplace_rating_aggregates.c.avg_rating <= request.rating_to
            )
        if request.seller_name:
            conditions.append(
                func.lower(cboxes.c.name).ilike(f"%{request.seller_name.lower()}%")
            )
        # seller_id НЕ используем для фильтрации, только для приоритизации
        # Если у селлера нет товаров - показываем все товары
        # Фильтрация по seller_id будет выполнена только в приоритизации
        if request.seller_phone:
            conditions.append(
                func.lower(users.c.phone_number).ilike(
                    f"%{request.seller_phone.lower()}%"
                )
            )

        if request.id:
            if request.id.isdigit():
                conditions.append(nomenclature.c.id == int(request.id))

        if request.name:
            conditions.append(
                func.lower(nomenclature.c.name).ilike(f"%{request.name.lower()}%")
            )

        if request.description_long:
            conditions.append(
                func.lower(nomenclature.c.description_long).ilike(
                    f"%{request.description_long.lower()}%"
                )
            )

        if request.seo_title:
            conditions.append(
                func.lower(nomenclature.c.seo_title).ilike(
                    f"%{request.seo_title.lower()}%"
                )
            )

        if request.seo_description:
            conditions.append(
                func.lower(nomenclature.c.seo_description).ilike(
                    f"%{request.seo_description.lower()}%"
                )
            )

        if request.seo_keywords:
            # func.unnest(Book.categories).column_valued()
            column_valued = func.unnest(nomenclature.c.seo_keywords).column_valued(
                "unnested_keywords"
            )
            # conditions.append(column_valued.ilike(f"%{request.seo_keywords.lower()}%"))
            conditions.append(
                select(column_valued)
                .where(column_valued.ilike(f"%{request.seo_keywords.lower()}%"))
                .exists()
            )

        if request.nomenclature_attributes:
            conditions.append(nomenclature.c.id == attrs_subquery.c.nomenclature_id)

        if request.global_category_id:
            # Получаем все ID категорий (включая дочерние) рекурсивно
            all_category_ids = await MarketplacePublicCategoriesService._get_all_category_ids_recursive(
                request.global_category_id
            )
            if all_category_ids:
                conditions.append(
                    nomenclature.c.global_category_id.in_(all_category_ids)
                )

        query = query.where(and_(*conditions))

        # --- GROUP BY — только по неизменяемым полям, без current_amount из balances ---
        group_by_fields = [
            nomenclature.c.id,
            units.c.convent_national_view,
            categories.c.name,
            manufacturers.c.name,
            active_prices_subquery.c.price,
            price_types.c.name,
            cboxes.c.seller_name,
            cboxes.c.name,
            cboxes.c.seller_photo,
            users.c.photo,
            cboxes.c.seller_description,
            marketplace_rating_aggregates.c.avg_rating,
            marketplace_rating_aggregates.c.reviews_count,
            stock_subquery.c.current_amount,
            total_sold_subquery.c.total_sold,
            cboxes.c.id,  # Нужно для проверки seller_id в sort_priority
        ]
        query = query.group_by(*group_by_fields)

        # --- Сортировка ---
        order = asc if request.sort_order == "asc" else desc

        # Приоритетная сортировка будет выполнена в Python после получения данных

        # Затем по выбранному полю
        if request.sort_by == MarketplaceSort.price:
            query = query.order_by(order(active_prices_subquery.c.price))
        elif request.sort_by == MarketplaceSort.name:
            query = query.order_by(order(nomenclature.c.name))
        elif request.sort_by == MarketplaceSort.rating:
            query = query.order_by(order(marketplace_rating_aggregates.c.avg_rating))
        elif request.sort_by == MarketplaceSort.total_sold:
            query = query.order_by(order(total_sold_subquery.c.total_sold))
        elif request.sort_by == MarketplaceSort.created_at:
            query = query.order_by(order(nomenclature.c.created_at))
        elif request.sort_by == MarketplaceSort.updated_at:
            query = query.order_by(order(nomenclature.c.updated_at))
        else:
            # по умолчанию — по продажам
            query = query.order_by(order(total_sold_subquery.c.total_sold))

        # Получаем все товары без пагинации для сортировки по приоритету в Python
        # (пагинацию применим после сортировки)
        products_db = await database.fetch_all(query)

        # --- Сортировка по приоритету в Python (город+селлер, город, селлер, остальные) ---
        if request.city or request.seller_id:
            # Получаем информацию о складах для определения города товара
            product_ids = [row["id"] for row in products_db]
            product_city_map = {}  # product_id -> has_city_warehouse (bool)

            if request.city and product_ids:
                # Проверяем, есть ли у товара склад в указанном городе
                city_check_query = (
                    select(
                        wb_latest.c.nomenclature_id.label("nomenclature_id"),
                        func.bool_or(
                            func.lower(wh_bal.c.address).ilike(
                                f"%{request.city.lower()}%"
                            )
                        ).label("has_city_warehouse"),
                    )
                    .select_from(
                        wb_latest.join(
                            wh_bal,
                            and_(
                                wh_bal.c.id == wb_latest.c.warehouse_id,
                                wh_bal.c.is_deleted.is_not(True),
                            ),
                        )
                    )
                    .where(
                        and_(
                            wb_latest.c.nomenclature_id.in_(product_ids),
                            wh_bal.c.address.is_not(None),
                        )
                    )
                    .group_by(wb_latest.c.nomenclature_id)
                )
                city_rows = await database.fetch_all(city_check_query)
                product_city_map = {
                    row["nomenclature_id"]: row["has_city_warehouse"]
                    for row in city_rows
                }

            # Проверяем, есть ли у селлера активные товары (если передан seller_id)
            seller_has_products = True
            if request.seller_id:
                seller_product_ids = [
                    row["id"]
                    for row in products_db
                    if (row.get("cashbox_id") or row.get("cashbox"))
                    == request.seller_id
                ]
                seller_has_products = len(seller_product_ids) > 0

            # Функция для определения приоритета товара
            def get_product_priority(product_row):
                cashbox_id = product_row.get("cashbox_id") or product_row.get("cashbox")
                has_city = product_city_map.get(product_row["id"], False)
                is_seller = (
                    (cashbox_id == request.seller_id) if request.seller_id else False
                )

                # Если у селлера нет товаров - не приоритизируем по seller_id
                if request.seller_id and not seller_has_products:
                    is_seller = False

                if has_city and is_seller:
                    return 0  # Город + селлер
                elif has_city:
                    return 1  # Только город
                elif is_seller:
                    return 2  # Только селлер
                else:
                    return 3  # Остальные

            # Сортируем по приоритету, затем по выбранному полю
            products_db = list(products_db)
            products_db.sort(key=lambda p: get_product_priority(p))

            # Затем применяем обычную сортировку по выбранному полю
            if request.sort_by == MarketplaceSort.price:
                products_db.sort(
                    key=lambda p: p.get("price", 0),
                    reverse=(request.sort_order != "asc"),
                )
            elif request.sort_by == MarketplaceSort.name:
                products_db.sort(
                    key=lambda p: p.get("name", ""),
                    reverse=(request.sort_order != "asc"),
                )
            elif request.sort_by == MarketplaceSort.rating:
                products_db.sort(
                    key=lambda p: p.get("rating") or 0,
                    reverse=(request.sort_order != "asc"),
                )
            elif request.sort_by == MarketplaceSort.total_sold:
                products_db.sort(
                    key=lambda p: p.get("total_sold", 0),
                    reverse=(request.sort_order != "asc"),
                )
            elif request.sort_by == MarketplaceSort.created_at:
                products_db.sort(
                    key=lambda p: p.get("created_at") or datetime.min,
                    reverse=(request.sort_order != "asc"),
                )
            elif request.sort_by == MarketplaceSort.updated_at:
                products_db.sort(
                    key=lambda p: p.get("updated_at") or datetime.min,
                    reverse=(request.sort_order != "asc"),
                )
            else:
                # По умолчанию — по продажам
                products_db.sort(
                    key=lambda p: p.get("total_sold", 0),
                    reverse=(request.sort_order != "asc"),
                )

            # Применяем пагинацию после сортировки
            offset = (request.page - 1) * request.size
            products_db = products_db[offset : offset + request.size]
        else:
            # Если нет параметров city/seller_id, применяем обычную пагинацию
            offset = (request.page - 1) * request.size
            products_db = products_db[offset : offset + request.size]

        # Получаем склады отдельным запросом только для тех товаров, которые вернулись
        # Это быстрее, чем делать это в основном запросе (избегаем дублирования строк)
        product_ids = [row["id"] for row in products_db]
        available_warehouses_map = {}

        if product_ids:
            # Используем wb_latest и wh_bal, которые были определены выше
            warehouses_query = (
                select(
                    wb_latest.c.nomenclature_id,
                    wh_bal.c.id.label("warehouse_id"),
                    wb_latest.c.organization_id,
                    wh_bal.c.name.label("warehouse_name"),
                    wh_bal.c.address.label("warehouse_address"),
                    wh_bal.c.latitude,
                    wh_bal.c.longitude,
                    wb_latest.c.current_amount,
                )
                .select_from(
                    wb_latest.join(
                        wh_bal,
                        and_(
                            wh_bal.c.id == wb_latest.c.warehouse_id,
                            wh_bal.c.is_public.is_(True),
                            wh_bal.c.status.is_(True),
                            wh_bal.c.is_deleted.is_not(True),
                        ),
                    )
                )
                .where(
                    and_(
                        wb_latest.c.nomenclature_id.in_(product_ids),
                        wb_latest.c.current_amount > 0,
                    )
                )
            )
            warehouses_rows = await database.fetch_all(warehouses_query)

            for row in warehouses_rows:
                nom_id = row["nomenclature_id"]
                if nom_id not in available_warehouses_map:
                    available_warehouses_map[nom_id] = []
                available_warehouses_map[nom_id].append(
                    {
                        "warehouse_id": row["warehouse_id"],
                        "organization_id": row["organization_id"],
                        "warehouse_name": row["warehouse_name"],
                        "warehouse_address": row["warehouse_address"],
                        "latitude": row["latitude"],
                        "longitude": row["longitude"],
                        "current_amount": row["current_amount"],
                    }
                )

        # --- Подсчёт общего количества (без дублей по складам / картинкам) ---
        count_query = (
            select(func.count(func.distinct(nomenclature.c.id)))
            .select_from(nomenclature)
            .join(categories, categories.c.id == nomenclature.c.category, isouter=True)
            .join(
                manufacturers,
                manufacturers.c.id == nomenclature.c.manufacturer,
                isouter=True,
            )
            .join(
                active_prices_subquery,
                active_prices_subquery.c.nomenclature_id == nomenclature.c.id,
            )
            .join(
                price_types,
                price_types.c.id == active_prices_subquery.c.price_type,
            )
            .join(
                marketplace_rating_aggregates,
                and_(
                    marketplace_rating_aggregates.c.entity_id == nomenclature.c.id,
                    marketplace_rating_aggregates.c.entity_type == "nomenclature",
                ),
                isouter=True,
            )
            .join(
                stock_subquery,
                stock_subquery.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )
            .where(and_(*conditions))
        )
        count_result = await database.fetch_one(count_query)
        total_count = count_result[0] if count_result else 0

        # --- Пост-обработка результатов ---
        products: List[MarketplaceProduct] = []
        for index, product in enumerate(products_db):
            product_dict = dict(product)
            product_dict["listing_pos"] = (request.page - 1) * request.size + index + 1
            product_dict["listing_page"] = request.page

            # Images - преобразуем ID фото в публичные URL
            images = product_dict.get("images")
            # PostgreSQL array_agg возвращает список или None
            if images is not None:
                # array_agg может вернуть список, даже если он пустой
                if isinstance(images, list):
                    # Фильтруем None значения и преобразуем ID в URL
                    image_ids = [pid for pid in images if pid is not None]
                    if image_ids:
                        product_dict["images"] = [
                            self.__build_picture_url(picture_id)
                            for picture_id in image_ids
                        ]
                        # Убираем None значения
                        product_dict["images"] = [
                            url for url in product_dict["images"] if url is not None
                        ] or None
                    else:
                        product_dict["images"] = None
                else:
                    product_dict["images"] = None
            else:
                product_dict["images"] = None

            # Barcodes
            barcodes = product_dict.get("barcodes")
            product_dict["barcodes"] = (
                [code for code in barcodes if code]
                if barcodes and any(barcodes)
                else None
            )

            # Список складов (только с остатком > 0)
            # Получаем из отдельного запроса, который мы выполнили выше
            wh_list = available_warehouses_map.get(product_dict["id"], [])
            if wh_list:
                product_dict["available_warehouses"] = sorted(
                    [
                        AvailableWarehouse(
                            **w,
                            distance_to_client=self._count_distance_to_client(
                                request.lat,
                                request.lon,
                                w["latitude"],
                                w["longitude"],
                            ),
                        )
                        for w in wh_list
                    ],
                    key=lambda x: (
                        x.distance_to_client is None,
                        x.distance_to_client or 0,
                    ),
                )
            else:
                product_dict["available_warehouses"] = None

            # Остальные поля
            product_dict["is_ad_pos"] = False
            product_dict["variations"] = []
            product_dict["distance"] = (
                min(
                    product_dict["available_warehouses"],
                    key=lambda x: x.distance_to_client,
                ).distance_to_client
                if product_dict["available_warehouses"]
                else None
            )
            product_dict["cashbox_id"] = product_dict["cashbox"]
            product_dict["seller_photo"] = self.__transform_photo_route(
                product_dict["seller_photo"]
            )

            product_button_text = product_buttons_text.get(product["type"]) or {}
            product_dict["button_text"] = product_button_text.get("name")
            product_dict["button_logic"] = product_button_text.get("logic")

            products.append(MarketplaceProduct(**product_dict))

        # Отдельная сортировка по distance (после расчёта distance_to_client)
        if request.sort_by == MarketplaceSort.distance:
            reverse = request.sort_order == "desc"
            products.sort(
                key=lambda x: x.distance if x.distance is not None else float("inf"),
                reverse=reverse,
            )

        return MarketplaceProductList(
            result=products,
            count=total_count,
            page=request.page,
            size=request.size,
        )

    async def _fetch_available_warehouses(
        self,
        nomenclature_id: int,
        client_lat: Optional[float] = None,
        client_lon: Optional[float] = None,
    ) -> List[AvailableWarehouse]:
        """
        Получает список публичных, активных и неудалённых складов,
        на которых есть остатки указанной номенклатуры,
        и возвращает их как AvailableWarehouse с расстоянием до клиента.
        """

        # Формируем JSON-объект для каждого склада
        json_obj = func.jsonb_build_object(
            literal_column("'warehouse_id'"),
            warehouses.c.id,
            literal_column("'organization_id'"),
            warehouse_balances.c.organization_id,
            literal_column("'warehouse_name'"),
            warehouses.c.name,
            literal_column("'warehouse_address'"),
            warehouses.c.address,
            literal_column("'latitude'"),
            warehouses.c.latitude,
            literal_column("'longitude'"),
            warehouses.c.longitude,
        )

        # Запрос: склады с остатками по указанной номенклатуре
        query = (
            select(json_obj)
            .select_from(warehouse_balances)
            .join(
                warehouses,
                and_(
                    warehouses.c.id == warehouse_balances.c.warehouse_id,
                    warehouses.c.is_public.is_(True),
                    warehouses.c.status.is_(True),
                    warehouses.c.is_deleted.is_not(True),
                ),
            )
            .where(
                and_(
                    warehouse_balances.c.nomenclature_id == nomenclature_id,
                    # Можно добавить условие на наличие остатка, если нужно:
                    # warehouse_balances.c.current_amount > 0
                )
            )
        )

        rows = await database.fetch_all(query)
        raw_warehouses = []
        for row in rows:
            if row and row[0]:
                # row[0] — это JSON-строка (str), нужно распарсить
                try:
                    wh_dict = json.loads(row[0])
                    raw_warehouses.append(wh_dict)
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue  # пропустить некорректные записи

        if not raw_warehouses:
            return []

        result = []
        for w in raw_warehouses:
            result.append(
                AvailableWarehouse(
                    **w,
                    distance_to_client=self._count_distance_to_client(
                        client_lat, client_lon, w["latitude"], w["longitude"]
                    ),
                )
            )

        # Сортируем: сначала склады с известным расстоянием (по возрастанию), потом — без координат
        result.sort(
            key=lambda x: (x.distance_to_client is None, x.distance_to_client or 0)
        )
        return result

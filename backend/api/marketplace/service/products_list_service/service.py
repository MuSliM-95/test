import json
import os
from datetime import datetime
from typing import List, Optional

from fastapi import HTTPException
from sqlalchemy import and_, desc, func, literal_column, select

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.products_list_service.schemas import (
    AvailableWarehouse,
    MarketplaceProduct,
    MarketplaceProductAttribute,
    MarketplaceProductDetail,
    MarketplaceProductList,
    MarketplaceProductsRequest,
    MarketplaceSort,
)
from database.db import (
    categories,
    cboxes,
    database,
    docs_sales,
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


class MarketplaceProductsListService(BaseMarketplaceService):
    @staticmethod
    def __transform_photo_route(photo_path: str) -> str:
        base_url = os.getenv("APP_URL")
        photo_url = photo_path.lstrip("/")

        if "seller" in photo_url:
            return f'https://{base_url}/api/v1/{photo_path.lstrip("/")}'
        else:
            return f'https://{base_url}/{photo_path.lstrip("/")}'

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
                func.array_agg(func.distinct(pictures.c.url))
                .filter(pictures.c.url.is_not(None))
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
            .join(users, users.c.id == cboxes.c.admin)
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
            .select_from(warehouse_balances)
            .join(
                warehouses,
                and_(
                    warehouses.c.id == warehouse_balances.c.warehouse_id,
                    # warehouses.c.status.is_(True),
                    warehouses.c.is_deleted.is_not(True),
                ),
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

        # 2. Для каждой группы — получаем вариации
        for group in groups:
            group_id = group["group_id"]
            group_name = group["group_name"]

            variations_query = (
                select(
                    nomenclature.c.id,
                    nomenclature.c.name,
                    nomenclature_groups_value.c.is_main,
                )
                .select_from(nomenclature_groups_value)
                .join(
                    nomenclature,
                    nomenclature.c.id == nomenclature_groups_value.c.nomenclature_id,
                )
                .where(nomenclature_groups_value.c.group_id == group_id)
            )

            variations = await database.fetch_all(variations_query)

            items = [
                {"id": v.id, "name": v.name, "is_main": v.is_main} for v in variations
            ]

            nomenclatures_result.append({"group_name": group_name, "items": items})

        product["nomenclatures"] = nomenclatures_result or None

        # Фото
        if product["images"]:
            product["images"] = [
                self.__transform_photo_route(url) for url in product["images"]
            ]

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

        # 1) Актуальная цена (ТОЛЬКО price_type = chatting)
        ranked_prices_subquery = (
            select(
                prices.c.id,
                prices.c.nomenclature,
                prices.c.price,
                prices.c.price_type,
                prices.c.created_at,
                prices.c.date_from,
                prices.c.date_to,
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
            .where(prices.c.is_deleted.is_not(True), price_types.c.name == "chatting")
            .subquery()
        )

        active_prices_subquery = (
            select(
                ranked_prices_subquery.c.nomenclature.label("nomenclature_id"),
                ranked_prices_subquery.c.price,
                ranked_prices_subquery.c.price_type,
            )
            .where(ranked_prices_subquery.c.rn == 1)
            .subquery()
        )

        # 2) Кол-во продаж (aggregate)
        total_sold_subquery = (
            select(
                docs_sales_goods.c.nomenclature.label("nomenclature_id"),
                func.count(docs_sales_goods.c.id).label("total_sold"),
            )
            .select_from(
                docs_sales_goods.join(
                    docs_sales, docs_sales.c.id == docs_sales_goods.c.docs_sales_id
                )
            )
            .where(docs_sales.c.is_deleted.is_not(True))
            .group_by(docs_sales_goods.c.nomenclature)
            .subquery()
        )

        # 3) Рейтинг (pre-aggregated table)
        mp_rating_subq = (
            select(
                marketplace_rating_aggregates.c.entity_id.label("nomenclature_id"),
                marketplace_rating_aggregates.c.avg_rating.label("rating"),
                marketplace_rating_aggregates.c.reviews_count.label("reviews_count"),
            )
            .where(marketplace_rating_aggregates.c.entity_type == "nomenclature")
            .subquery()
        )

        # 4) Условия фильтрации
        conditions = [nomenclature.c.is_deleted.is_not(True)]

        if request.category:
            conditions.append(categories.c.name.ilike(f"%{request.category}%"))

        if request.manufacturer:
            conditions.append(manufacturers.c.name.ilike(f"%{request.manufacturer}%"))

        if request.seller_id is not None:
            conditions.append(cboxes.c.id == request.seller_id)

        if request.seller_name:
            conditions.append(
                func.coalesce(
                    func.nullif(cboxes.c.seller_name, ""), cboxes.c.name
                ).ilike(f"%{request.seller_name}%")
            )

        if request.seller_phone:
            conditions.append(users.c.phone_number.ilike(f"%{request.seller_phone}%"))

        if request.min_price is not None:
            conditions.append(active_prices_subquery.c.price >= request.min_price)

        if request.max_price is not None:
            conditions.append(active_prices_subquery.c.price <= request.max_price)

        if request.rating_from is not None:
            conditions.append(mp_rating_subq.c.rating >= request.rating_from)

        if request.rating_to is not None:
            conditions.append(mp_rating_subq.c.rating <= request.rating_to)

        # stock-фильтр: используем тяжёлую агрегацию ТОЛЬКО когда он реально нужен
        need_stock_filter = bool(request.in_stock)

        stock_subquery = None
        if need_stock_filter:
            # Важно: фильтруем остатки только по публичным, активным и неудалённым складам,
            # иначе in_stock может сработать на "закрытых" складах.
            wh_filter = warehouses.alias("wh_filter")

            wb_ranked = (
                select(
                    warehouse_balances.c.organization_id,
                    warehouse_balances.c.warehouse_id,
                    warehouse_balances.c.nomenclature_id,
                    warehouse_balances.c.current_amount,
                    wh_filter.c.is_public.label("warehouse_is_public"),
                    wh_filter.c.status.label("warehouse_status"),
                    wh_filter.c.is_deleted.label("warehouse_is_deleted"),
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
                )
                .select_from(
                    warehouse_balances.join(
                        wh_filter, wh_filter.c.id == warehouse_balances.c.warehouse_id
                    )
                )
                .subquery()
            )

            wb_latest = (
                select(
                    wb_ranked.c.nomenclature_id,
                    wb_ranked.c.current_amount,
                )
                .where(
                    and_(
                        wb_ranked.c.rn == 1,
                        wb_ranked.c.current_amount > 0,
                        wb_ranked.c.warehouse_is_public.is_(True),
                        wb_ranked.c.warehouse_status.is_(True),
                        wb_ranked.c.warehouse_is_deleted.is_not(True),
                    )
                )
                .subquery()
            )

            stock_subquery = (
                select(
                    wb_latest.c.nomenclature_id,
                    func.sum(func.greatest(wb_latest.c.current_amount, 0)).label(
                        "current_amount"
                    ),
                )
                .group_by(wb_latest.c.nomenclature_id)
                .subquery()
            )

            conditions.append(stock_subquery.c.current_amount > 0)

        # 5) ОСНОВНОЙ запрос (без pictures/barcodes/warehouses)
        base_query = (
            select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.description_short,
                nomenclature.c.description_long,
                nomenclature.c.code,
                units.c.convent_national_view.label("unit_name"),
                cboxes.c.id.label("cashbox_id"),
                categories.c.name.label("category_name"),
                manufacturers.c.name.label("manufacturer_name"),
                active_prices_subquery.c.price.label("price"),
                # price_type всегда chatting по задаче
                literal_column("'chatting'").label("price_type"),
                nomenclature.c.created_at,
                nomenclature.c.updated_at,
                nomenclature.c.type,
                func.coalesce(mp_rating_subq.c.rating, None).label("rating"),
                func.coalesce(mp_rating_subq.c.reviews_count, None).label(
                    "reviews_count"
                ),
                func.coalesce(total_sold_subquery.c.total_sold, 0).label("total_sold"),
                (
                    func.coalesce(stock_subquery.c.current_amount, 0)
                    if need_stock_filter
                    else literal_column("0")
                ).label("current_amount"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_name, ""), cboxes.c.name
                ).label("seller_name"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_photo, ""), users.c.photo
                ).label("seller_photo"),
                cboxes.c.seller_description.label("seller_description"),
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
            .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
            .join(users, users.c.id == cboxes.c.admin, isouter=True)
            .join(
                mp_rating_subq,
                mp_rating_subq.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )
            .join(
                total_sold_subquery,
                total_sold_subquery.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )
        )

        if need_stock_filter:
            base_query = base_query.join(
                stock_subquery,
                stock_subquery.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )

        base_query = base_query.where(and_(*conditions))

        # 6) Сортировка
        sort_by = request.sort_by or MarketplaceSort.total_sold
        sort_order = request.sort_order or "desc"

        sort_columns = {
            MarketplaceSort.price: active_prices_subquery.c.price,
            MarketplaceSort.name: nomenclature.c.name,
            MarketplaceSort.rating: mp_rating_subq.c.rating,
            MarketplaceSort.total_sold: total_sold_subquery.c.total_sold,
            MarketplaceSort.created_at: nomenclature.c.created_at,
            MarketplaceSort.updated_at: nomenclature.c.updated_at,
        }

        # distance сортируем после расчёта
        sort_col = sort_columns.get(sort_by, total_sold_subquery.c.total_sold)

        if sort_order == "asc":
            base_query = base_query.order_by(sort_col.asc().nullslast())
        else:
            base_query = base_query.order_by(sort_col.desc().nullslast())

        # 7) Пагинация
        offset = (request.page - 1) * request.size
        base_query = base_query.limit(request.size).offset(offset)

        products_db = await database.fetch_all(base_query)

        # 8) COUNT
        count_query = (
            select(func.count(func.distinct(nomenclature.c.id)).label("count"))
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
            .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
            .join(users, users.c.id == cboxes.c.admin, isouter=True)
            .join(
                mp_rating_subq,
                mp_rating_subq.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )
        )

        if need_stock_filter:
            count_query = count_query.join(
                stock_subquery,
                stock_subquery.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )

        count_query = count_query.where(and_(*conditions))

        total_count_row = await database.fetch_one(count_query)
        total_count = int(total_count_row["count"]) if total_count_row else 0

        # 9) ДОЗАПРОСЫ по ids: images / barcodes / warehouses
        nomenclature_ids = [row["id"] for row in products_db]

        images_map = {}
        barcodes_map = {}
        warehouses_map = {}  # nid -> list[AvailableWarehouse]
        stock_map = {}  # nid -> current_amount

        if nomenclature_ids:
            # images
            images_rows = await database.fetch_all(
                select(
                    pictures.c.entity_id.label("nomenclature_id"),
                    func.array_agg(func.distinct(pictures.c.url)).label("images"),
                )
                .where(
                    and_(
                        pictures.c.entity == "nomenclature",
                        pictures.c.is_deleted.is_not(True),
                        pictures.c.entity_id.in_(nomenclature_ids),
                    )
                )
                .group_by(pictures.c.entity_id)
            )
            for r in images_rows:
                images_map[r["nomenclature_id"]] = r["images"] or []

            # barcodes
            barcodes_rows = await database.fetch_all(
                select(
                    nomenclature_barcodes.c.nomenclature_id,
                    func.array_agg(func.distinct(nomenclature_barcodes.c.code)).label(
                        "barcodes"
                    ),
                )
                .where(nomenclature_barcodes.c.nomenclature_id.in_(nomenclature_ids))
                .group_by(nomenclature_barcodes.c.nomenclature_id)
            )
            for r in barcodes_rows:
                barcodes_map[r["nomenclature_id"]] = r["barcodes"] or []

            # warehouses (только для выбранных ids)
            wh = warehouses.alias("wh")
            wb_ranked_page = (
                select(
                    warehouse_balances.c.organization_id,
                    warehouse_balances.c.warehouse_id,
                    warehouse_balances.c.nomenclature_id,
                    warehouse_balances.c.current_amount,
                    wh.c.name.label("warehouse_name"),
                    wh.c.address.label("warehouse_address"),
                    wh.c.latitude.label("warehouse_latitude"),
                    wh.c.longitude.label("warehouse_longitude"),
                    wh.c.is_public.label("warehouse_is_public"),
                    wh.c.status.label("warehouse_status"),
                    wh.c.is_deleted.label("warehouse_is_deleted"),
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
                )
                .select_from(
                    warehouse_balances.join(
                        wh, wh.c.id == warehouse_balances.c.warehouse_id
                    )
                )
                .where(warehouse_balances.c.nomenclature_id.in_(nomenclature_ids))
                .subquery()
            )

            wb_latest_rows = await database.fetch_all(
                select(wb_ranked_page).where(
                    and_(
                        wb_ranked_page.c.rn == 1,
                        wb_ranked_page.c.current_amount > 0,
                        wb_ranked_page.c.warehouse_is_public.is_(True),
                        wb_ranked_page.c.warehouse_status.is_(True),
                        wb_ranked_page.c.warehouse_is_deleted.is_not(True),
                    )
                )
            )

            for r in wb_latest_rows:
                nid = r["nomenclature_id"]
                stock_map[nid] = stock_map.get(nid, 0.0) + float(
                    r["current_amount"] or 0
                )

                wh_item = AvailableWarehouse(
                    warehouse_id=r["warehouse_id"],
                    organization_id=r["organization_id"],
                    warehouse_name=r["warehouse_name"],
                    warehouse_address=r["warehouse_address"],
                    latitude=r["warehouse_latitude"],
                    longitude=r["warehouse_longitude"],
                    distance_to_client=None,
                    current_amount=float(r["current_amount"] or 0),
                )
                warehouses_map.setdefault(nid, []).append(wh_item)

        # 10) Сборка ответа
        response_products = []
        for row in products_db:
            product_dict = dict(row)
            nid = product_dict["id"]

            imgs = images_map.get(nid, [])
            product_dict["images"] = (
                [self.__transform_photo_route(img) for img in imgs] if imgs else None
            )

            bcs = barcodes_map.get(nid, [])
            product_dict["barcodes"] = bcs if bcs else None

            product_dict["current_amount"] = stock_map.get(
                nid, float(product_dict.get("current_amount") or 0)
            )
            product_dict["available_warehouses"] = warehouses_map.get(nid) or None

            if product_dict.get("seller_photo"):
                product_dict["seller_photo"] = self.__transform_photo_route(
                    product_dict["seller_photo"]
                )

            # distance (минимальная дистанция из доступных складов)
            if (
                request.lat is not None
                and request.lon is not None
                and product_dict.get("available_warehouses")
            ):
                distances = []
                for wh_item in product_dict["available_warehouses"]:
                    if wh_item.latitude is not None and wh_item.longitude is not None:
                        wh_item.distance_to_client = self._count_distance_to_client(
                            request.lat,
                            request.lon,
                            wh_item.latitude,
                            wh_item.longitude,
                        )
                        distances.append(wh_item.distance_to_client)
                product_dict["distance"] = min(distances) if distances else None
            else:
                product_dict["distance"] = None

            # поля для фронта (как и раньше)
            product_dict["listing_pos"] = None
            product_dict["listing_page"] = request.page
            product_dict["is_ad_pos"] = False
            product_dict["tags"] = []
            product_dict["variations"] = []

            response_products.append(MarketplaceProduct(**product_dict))

        # Сортировка по distance после расчёта
        if request.sort_by == MarketplaceSort.distance:
            # Важно: None (нет координат/складов) всегда уводим в конец,
            # независимо от направления сортировки.
            if sort_order == "asc":
                response_products.sort(
                    key=lambda p: (p.distance is None, p.distance or 0),
                )
            else:
                response_products.sort(
                    key=lambda p: (p.distance is None, -(p.distance or 0)),
                )

        return MarketplaceProductList(
            result=response_products,
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

import json
import os
from datetime import datetime
from typing import Optional, List
from fastapi import HTTPException

from sqlalchemy import and_, select, func, asc, desc, literal_column, cast
from sqlalchemy.dialects.postgresql import JSONB

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.products_list_service.schemas import MarketplaceProduct, MarketplaceProductDetail, \
    MarketplaceProductAttribute, MarketplaceProductList, AvailableWarehouse, MarketplaceProductsRequest, MarketplaceSort
from database.db import nomenclature, prices, price_types, database, warehouses, warehouse_balances, units, categories, \
    manufacturers, cboxes, marketplace_rating_aggregates, pictures, nomenclature_barcodes, users, docs_sales_goods, \
    docs_sales, nomenclature_attributes, nomenclature_attributes_value, nomenclature_groups_value


class MarketplaceProductsListService(BaseMarketplaceService):
    @staticmethod
    def __transform_photo_route(photo_path: str) -> str:
        base_url = os.getenv("APP_URL")
        return f'https://{base_url}/api/v1/{photo_path.lstrip("/")}'

    async def get_product(self, product_id: int) -> MarketplaceProductDetail:

        current_timestamp = int(datetime.now().timestamp())

        ranked_prices_subquery = (
            select(
                prices.c.nomenclature.label('nomenclature_id'),
                prices.c.id.label('price_id'),
                prices.c.price,
                prices.c.price_type,
                prices.c.created_at,
                prices.c.date_from,
                prices.c.date_to,
                prices.c.is_deleted,
                func.row_number().over(
                    partition_by=prices.c.nomenclature,
                    order_by=[
                        desc(func.coalesce(prices.c.date_from <= current_timestamp, True)
                             & func.coalesce(current_timestamp < prices.c.date_to, True)),
                        desc(prices.c.created_at),
                        desc(prices.c.id)
                    ]
                ).label('rn')
            )
            .where(prices.c.is_deleted.is_not(True))
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
                func.count(docs_sales_goods.c.id).label("total_sold")
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

                cboxes.c.name.label("seller_name"),
                users.c.photo.label("seller_photo"),

                marketplace_rating_aggregates.c.avg_rating.label("rating"),
                marketplace_rating_aggregates.c.reviews_count.label("reviews_count"),

                func.array_agg(func.distinct(pictures.c.url))
                .filter(pictures.c.url.is_not(None)).label("images"),

                func.array_agg(func.distinct(nomenclature_barcodes.c.code))
                .filter(nomenclature_barcodes.c.code.is_not(None)).label("barcodes"),

                func.coalesce(total_sold_subquery.c.total_sold, 0).label("total_sold"),
            )
            .select_from(nomenclature)
            .join(units, units.c.id == nomenclature.c.unit, isouter=True)
            .join(categories, categories.c.id == nomenclature.c.category, isouter=True)
            .join(manufacturers, manufacturers.c.id == nomenclature.c.manufacturer, isouter=True)
            .join(active_prices_subquery, active_prices_subquery.c.nomenclature_id == nomenclature.c.id)
            .join(price_types, price_types.c.id == active_prices_subquery.c.price_type)
            .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
            .join(users, users.c.id == cboxes.c.admin)
            .join(pictures, and_(
                pictures.c.entity == "nomenclature",
                pictures.c.entity_id == nomenclature.c.id,
                pictures.c.is_deleted.is_not(True)
            ), isouter=True)
            .join(nomenclature_barcodes, nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id, isouter=True)
            .join(
                marketplace_rating_aggregates,
                and_(
                    marketplace_rating_aggregates.c.entity_id == nomenclature.c.id,
                    marketplace_rating_aggregates.c.entity_type == "nomenclature"
                ),
                isouter=True
            )
            .join(total_sold_subquery, total_sold_subquery.c.nomenclature == nomenclature.c.id, isouter=True)
            .where(
                and_(
                    nomenclature.c.id == product_id,
                    nomenclature.c.is_deleted.is_not(True),
                    price_types.c.name == "chatting"
                )
            )
            .group_by(
                nomenclature.c.id,
                units.c.convent_national_view,
                categories.c.name,
                manufacturers.c.name,
                active_prices_subquery.c.price,
                price_types.c.name,
                cboxes.c.name,
                users.c.photo,
                marketplace_rating_aggregates.c.avg_rating,
                marketplace_rating_aggregates.c.reviews_count,
                total_sold_subquery.c.total_sold
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
                warehouse_balances.c.organization_id
            )
            .select_from(warehouse_balances)
            .join(warehouses, and_(
                warehouses.c.id == warehouse_balances.c.warehouse_id,
                # warehouses.c.status.is_(True),
                warehouses.c.is_deleted.is_not(True)
            ))
            .where(and_(
                warehouse_balances.c.nomenclature_id == product_id,
                warehouse_balances.c.current_amount > 0  # Только склады с остатками
            ))
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
                    distance_to_client=self._count_distance_to_client(None, None, wh_dict["latitude"],
                                                                      wh_dict["longitude"])
                )
            )

        product["available_warehouses"] = available_warehouses or None
        product["current_amount"] = total_amount

        # Вариации товаров
        group_query = select(
            nomenclature_groups_value.c.group_id
        ).where(
            nomenclature_groups_value.c.nomenclature_id == product_id
        )

        group_row = await database.fetch_one(group_query)
        nomenclatures_list = []

        if group_row and group_row["group_id"]:
            group_id = group_row["group_id"]

            variations_query = (
                select(
                    nomenclature.c.id,
                    nomenclature.c.name,
                    nomenclature_groups_value.c.is_main
                )
                .select_from(nomenclature_groups_value)
                .join(
                    nomenclature,
                    nomenclature.c.id == nomenclature_groups_value.c.nomenclature_id
                )
                .where(nomenclature_groups_value.c.group_id == group_id)
            )

            variations = await database.fetch_all(variations_query)

            nomenclatures_list = [
                {
                    "id": v.id,
                    "name": v.name,
                    "is_main": v.is_main
                }
                for v in variations
            ]

        product["nomenclatures"] = nomenclatures_list

        # Фото
        if product["images"]:
            product["images"] = [
                self.__transform_photo_route(url) for url in product["images"]
            ]

        # Селлер
        if product["seller_photo"]:
            product["seller_photo"] = self.__transform_photo_route(product["seller_photo"])

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
            product["distance"] = (
                min(
                    product["available_warehouses"],
                    key=lambda x: (x.distance_to_client is None, x.distance_to_client or 0)
                ).distance_to_client
            )
        else:
            product["distance"] = None

        # Добавляем атрибуты
        attrs = await database.fetch_all(
            select(
                nomenclature_attributes.c.name,
                nomenclature_attributes_value.c.value
            )
            .select_from(nomenclature_attributes_value)
            .join(nomenclature_attributes, nomenclature_attributes.c.id == nomenclature_attributes_value.c.attribute_id)
            .where(nomenclature_attributes_value.c.nomenclature_id == product_id)
        )
        product_attributes = [
            MarketplaceProductAttribute(name=a.name, value=a.value)
            for a in attrs
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
                prices.c.nomenclature.label('nomenclature_id'),
                prices.c.id.label('price_id'),
                prices.c.price,
                prices.c.price_type,
                prices.c.created_at,
                prices.c.date_from,
                prices.c.date_to,
                prices.c.is_deleted,
                # Ранжируем: 1. по времени (текущее в интервале), 2. по дате создания (новые первые), 3. по id (стабильность)
                func.row_number().over(
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
                        desc(func.coalesce(prices.c.date_from <= current_timestamp, True) & func.coalesce(current_timestamp < prices.c.date_to, True)),
                        # Потом по дате создания (новые первые)
                        desc(prices.c.created_at),
                        # Потом по ID (стабильность)
                        desc(prices.c.id)
                    ]
                ).label('rn')
            )
            .where(
                # Учитываем только неудаленные цены
                prices.c.is_deleted.is_not(True)
            )
            .subquery()
        )

        total_sold_subquery = (
            select(
                docs_sales_goods.c.nomenclature,
                func.count(docs_sales_goods.c.id).label("total_sold")
            )
            .select_from(docs_sales_goods)
            .group_by(docs_sales_goods.c.nomenclature)
            .subquery()
        )

        # Фильтруем подзапрос, чтобы оставить только первую (самую приоритетную) цену для каждой номенклатуры
        # Это будет "актуальная" цена
        active_prices_subquery = select(ranked_prices_subquery).where(ranked_prices_subquery.c.rn == 1).subquery()

        # --- КОНЕЦ: Подзапрос для выбора актуальной цены ---

        # Алиас для складов, связанных с балансами (чтобы не конфликтовать с основным warehouses)
        wh_bal = warehouses.alias("wh_bal")

        json_obj = func.jsonb_build_object(
            literal_column("'warehouse_id'"), wh_bal.c.id,
            literal_column("'organization_id'"), warehouse_balances.c.organization_id,
            literal_column("'warehouse_name'"), wh_bal.c.name,
            literal_column("'warehouse_address'"), wh_bal.c.address,
            literal_column("'latitude'"), wh_bal.c.latitude,
            literal_column("'longitude'"), wh_bal.c.longitude
        )

        available_warehouses_agg = (
            func.array_agg(cast(json_obj, JSONB).distinct())
            .filter(wh_bal.c.id.is_not(None))
            .label("available_warehouses")
        )

        # Используем active_prices_subquery вместо prices напрямую
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
                units.c.convent_national_view.label("unit_name"),
                categories.c.name.label("category_name"),
                manufacturers.c.name.label("manufacturer_name"),
                # Теперь выбираем цену из подзапроса
                active_prices_subquery.c.price,
                price_types.c.name.label("price_type"),
                cboxes.c.name.label("seller_name"),
                users.c.photo.label("seller_photo"),
                marketplace_rating_aggregates.c.avg_rating.label("rating"),
                marketplace_rating_aggregates.c.reviews_count.label("reviews_count"),
                func.array_agg(
                    func.distinct(pictures.c.url)
                ).filter(pictures.c.url.is_not(None)).label("images"),
                func.array_agg(
                    func.distinct(nomenclature_barcodes.c.code)
                ).filter(nomenclature_barcodes.c.code.is_not(None)).label("barcodes"),
                warehouse_balances.c.current_amount,
                func.coalesce(total_sold_subquery.c.total_sold, 0).label("total_sold"), # Используем coalesce, чтобы получить 0, если нет продаж
                available_warehouses_agg
            )
            .select_from(nomenclature)
            .join(units, units.c.id == nomenclature.c.unit, isouter=True)
            .join(categories, categories.c.id == nomenclature.c.category, isouter=True)
            .join(manufacturers, manufacturers.c.id == nomenclature.c.manufacturer, isouter=True)
            # JOIN к подзапросу актуальных цен
            .join(active_prices_subquery, active_prices_subquery.c.nomenclature_id == nomenclature.c.id)
            .join(price_types, price_types.c.id == active_prices_subquery.c.price_type) # Теперь JOIN к price_types через подзапрос
            .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
            .join(users, users.c.id == cboxes.c.admin)
            .join(pictures, and_(
                pictures.c.entity == "nomenclature",
                pictures.c.entity_id == nomenclature.c.id,
                pictures.c.is_deleted.is_not(True)
            ), isouter=True)
            .join(nomenclature_barcodes, nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id, isouter=True)
            .join(warehouses, warehouses.c.cashbox == nomenclature.c.cashbox)
            .join(
                marketplace_rating_aggregates,
                and_(
                    marketplace_rating_aggregates.c.entity_id == nomenclature.c.id,
                    marketplace_rating_aggregates.c.entity_type == "nomenclature"
                ),
                isouter=True
            )
            # Warehouses for balances (public only)
            .join(warehouse_balances, and_(
                warehouse_balances.c.nomenclature_id == nomenclature.c.id,
            ), isouter=True)
            .join(wh_bal, and_(
                wh_bal.c.id == warehouse_balances.c.warehouse_id,
                wh_bal.c.is_public.is_(True),
                wh_bal.c.status.is_(True),
                wh_bal.c.is_deleted.is_not(True)
            ), isouter=True)
            .join(total_sold_subquery, total_sold_subquery.c.nomenclature == nomenclature.c.id, isouter=True)
        )

        conditions = [
            nomenclature.c.is_deleted.is_not(True),
            # Условие price_types.name == "chatting" теперь относится к подзапросу через price_types
            price_types.c.name == "chatting",
        ]

        if request.category:
            conditions.append(categories.c.name.ilike(f"%{request.category}%"))
        if request.manufacturer:
            conditions.append(manufacturers.c.name.ilike(f"%{request.manufacturer}%"))
        if request.min_price is not None:
            # Теперь min_price/max_price применяются к цене из подзапроса
            conditions.append(active_prices_subquery.c.price >= request.min_price)
        if request.max_price is not None:
            conditions.append(active_prices_subquery.c.price <= request.max_price)
        if request.in_stock:
            conditions.append(warehouse_balances.c.current_amount.is_not(None))
            conditions.append(warehouse_balances.c.current_amount > 0)
        if request.rating_from:
            conditions.append(marketplace_rating_aggregates.c.avg_rating >= request.rating_from)
        if request.rating_to:
            conditions.append(marketplace_rating_aggregates.c.avg_rating <= request.rating_to)

        # if request.tags is not None:
        #     conditions.append()
        query = query.where(and_(*conditions))

        group_by_fields = [
            nomenclature.c.id,
            units.c.convent_national_view,
            categories.c.name,
            manufacturers.c.name,
            active_prices_subquery.c.price,
            price_types.c.name,
            cboxes.c.name,
            users.c.photo,
            marketplace_rating_aggregates.c.avg_rating,
            marketplace_rating_aggregates.c.reviews_count,
            warehouse_balances.c.current_amount,
            total_sold_subquery.c.total_sold
        ]
        query = query.group_by(*group_by_fields)
        order = asc if request.sort_order == 'asc' else desc

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
            query = query.order_by(order(total_sold_subquery.c.total_sold))

        offset = (request.page - 1) * request.size
        query = query.limit(request.size).offset(offset)

        products_db = await database.fetch_all(query)

        # Подсчёт общего количества (без пагинации)
        # count_query также должен использовать подзапрос для уникальности
        count_query = (
            select(func.count(nomenclature.c.id))
            .select_from(nomenclature)
            # JOIN к подзапросу актуальных цен
            .join(active_prices_subquery, active_prices_subquery.c.nomenclature_id == nomenclature.c.id)
            .join(price_types, price_types.c.id == active_prices_subquery.c.price_type)
            .where(and_(*conditions)) # Условия те же, но без сортировки
        )
        count_result = await database.fetch_one(count_query)
        total_count = count_result[0] if count_result else 0

        products: List[MarketplaceProduct] = []
        for index, product in enumerate(products_db):
            product_dict = dict(product)
            product_dict["listing_pos"] = (request.page - 1) * request.size + index + 1
            product_dict["listing_page"] = request.page

            # Images
            images = product_dict.get("images")
            product_dict["images"] = [self.__transform_photo_route(url) for url in images if url] if images and any(images) else None

            # Barcodes
            barcodes = product_dict.get("barcodes")
            product_dict["barcodes"] = [code for code in barcodes if code] if barcodes and any(barcodes) else None

            wh_raw = product_dict.get("available_warehouses")
            if wh_raw and isinstance(wh_raw, list):
                wh_valid = [w for w in wh_raw if w is not None and w.get("warehouse_id") is not None]
                if wh_valid:
                    product_dict["available_warehouses"] = sorted([
                        AvailableWarehouse(**w, distance_to_client=self._count_distance_to_client(request.lat, request.lon,
                                                                                                  w['latitude'],
                                                                                                  w['longitude']))
                        for w in wh_valid
                    ], key=lambda x: (x.distance_to_client is None, x.distance_to_client or 0))
                else:
                    product_dict["available_warehouses"] = None
            else:
                product_dict["available_warehouses"] = None

            # Остальные поля
            product_dict["is_ad_pos"] = False
            product_dict["variations"] = []
            product_dict["distance"] = min(product_dict["available_warehouses"], key=lambda x: x.distance_to_client).distance_to_client if product_dict["available_warehouses"] else None
            product_dict["cashbox_id"] = product_dict["cashbox"]
            product_dict["seller_photo"] = self.__transform_photo_route(product_dict["seller_photo"])

            products.append(MarketplaceProduct(**product_dict))

        if request.sort_by == MarketplaceSort.distance:
            products.sort(key=lambda x: (1 if (not request.sort_order or request.sort_by == 'asc') else -1) * (x.distance or float('inf')))

        return MarketplaceProductList(
            result=products,
            count=total_count,
            page=request.page,
            size=request.size
        )


    async def _fetch_available_warehouses(
            self,
            nomenclature_id: int,
            client_lat: Optional[float] = None,
            client_lon: Optional[float] = None
    ) -> List[AvailableWarehouse]:
        """
        Получает список публичных, активных и неудалённых складов,
        на которых есть остатки указанной номенклатуры,
        и возвращает их как AvailableWarehouse с расстоянием до клиента.
        """

        # Формируем JSON-объект для каждого склада
        json_obj = func.jsonb_build_object(
            literal_column("'warehouse_id'"), warehouses.c.id,
            literal_column("'organization_id'"), warehouse_balances.c.organization_id,
            literal_column("'warehouse_name'"), warehouses.c.name,
            literal_column("'warehouse_address'"), warehouses.c.address,
            literal_column("'latitude'"), warehouses.c.latitude,
            literal_column("'longitude'"), warehouses.c.longitude
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
                    warehouses.c.is_deleted.is_not(True)
                )
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
            result.append(AvailableWarehouse(**w, distance_to_client=self._count_distance_to_client(client_lat,
                                                                                                    client_lon,
                                                                                                    w['latitude'],
                                                                                                    w['longitude'])))

        # Сортируем: сначала склады с известным расстоянием (по возрастанию), потом — без координат
        result.sort(key=lambda x: (x.distance_to_client is None, x.distance_to_client or 0))
        return result

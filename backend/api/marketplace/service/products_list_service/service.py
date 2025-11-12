import json
import math
from typing import Optional, List
from datetime import datetime

# Допустим, вы используете UTC. Импортируйте нужный способ получения текущего времени.
from sqlalchemy import and_, select, func, asc, desc, literal_column, JSON, cast, text
from sqlalchemy.dialects.postgresql import JSONB

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.products_list_service.schemas import MarketplaceProduct, MarketplaceProductList, \
    AvailableWarehouse
from database.db import nomenclature, prices, price_types, database, warehouses, warehouse_balances, units, categories, \
    manufacturers, cboxes, marketplace_rating_aggregates, pictures, nomenclature_barcodes

# Дополнительный импорт для работы с датами/временем, если нужно явно указать UTC
from sqlalchemy.sql import func as sql_func
# from sqlalchemy.dialects.postgresql import INTERVAL # Если нужно учитывать timezone

class MarketplaceProductsListService(BaseMarketplaceService):
    @staticmethod
    def __count_distance_to_client(client_lat: Optional[float], client_long: Optional[float], warehouse_lat: Optional[float], warehouse_long: Optional[float]) -> Optional[float]:
        if not all([client_lat, client_long, warehouse_lat, warehouse_long]):
            return None

        R = 6371.0  # радиус Земли в километрах

        lat1_rad = math.radians(client_lat)
        lon1_rad = math.radians(client_long)
        lat2_rad = math.radians(warehouse_lat)
        lon2_rad = math.radians(warehouse_long)

        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        distance = R * c
        return distance  # в километрах

    async def get_products(
            self,
            phone: Optional[str],
            lat: Optional[float],
            lon: Optional[float],
            city: Optional[str],
            page: int,
            size: int,
            sort: Optional[str],
            category_filter: Optional[str],
            manufacturer_filter: Optional[str],
            min_price: Optional[float],
            max_price: Optional[float],
            tags_filter: Optional[str],
            in_stock: Optional[bool],
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
                units.c.convent_national_view.label("unit_name"),
                categories.c.name.label("category_name"),
                manufacturers.c.name.label("manufacturer_name"),
                # Теперь выбираем цену из подзапроса
                active_prices_subquery.c.price,
                price_types.c.name.label("price_type"),
                cboxes.c.name.label("seller_name"),
                marketplace_rating_aggregates.c.avg_rating.label("rating"),
                marketplace_rating_aggregates.c.reviews_count.label("reviews_count"),
                func.array_agg(
                    func.distinct(pictures.c.url)
                ).filter(pictures.c.url.is_not(None)).label("images"),
                func.array_agg(
                    func.distinct(nomenclature_barcodes.c.code)
                ).filter(nomenclature_barcodes.c.code.is_not(None)).label("barcodes"),
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
                # warehouse_balances.c.current_amount >= 0
            ), isouter=True)
            .join(wh_bal, and_(
                wh_bal.c.id == warehouse_balances.c.warehouse_id,
                wh_bal.c.is_public.is_(True),
                wh_bal.c.status.is_(True),
                wh_bal.c.is_deleted.is_not(True)
            ), isouter=True)
        )

        conditions = [
            nomenclature.c.is_deleted.is_not(True),
            # Условие price_types.name == "chatting" теперь относится к подзапросу через price_types
            price_types.c.name == "chatting",
        ]

        if category_filter:
            conditions.append(categories.c.name.ilike(f"%{category_filter}%"))
        if manufacturer_filter:
            conditions.append(manufacturers.c.name.ilike(f"%{manufacturer_filter}%"))
        if min_price is not None:
            # Теперь min_price/max_price применяются к цене из подзапроса
            conditions.append(active_prices_subquery.c.price >= min_price)
        if max_price is not None:
            conditions.append(active_prices_subquery.c.price <= max_price)

        query = query.where(and_(*conditions))

        group_by_fields = [
            nomenclature.c.id,
            units.c.convent_national_view,
            categories.c.name,
            manufacturers.c.name,
            # Теперь группировка по цене из подзапроса
            active_prices_subquery.c.price,
            price_types.c.name,
            cboxes.c.name,
            marketplace_rating_aggregates.c.avg_rating,
            marketplace_rating_aggregates.c.reviews_count,
        ]
        query = query.group_by(*group_by_fields)

        if sort == "price":
            # Сортировка теперь по цене из подзапроса
            query = query.order_by(asc(active_prices_subquery.c.price))
        elif sort == "name":
            query = query.order_by(asc(nomenclature.c.name))
        elif sort == "created_at":
            query = query.order_by(desc(nomenclature.c.created_at))
        else:
            query = query.order_by(desc(nomenclature.c.id))

        offset = (page - 1) * size
        query = query.limit(size).offset(offset)

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
            product_dict["listing_pos"] = (page - 1) * size + index + 1
            product_dict["listing_page"] = page

            # Images
            images = product_dict.get("images")
            product_dict["images"] = [url for url in images if url] if images and any(images) else None

            # Barcodes
            barcodes = product_dict.get("barcodes")
            product_dict["barcodes"] = [code for code in barcodes if code] if barcodes and any(barcodes) else None

            wh_raw = product_dict.get("available_warehouses")
            if wh_raw and isinstance(wh_raw, list):
                wh_valid = [w for w in wh_raw if w is not None and w.get("warehouse_id") is not None]
                if wh_valid:
                    product_dict["available_warehouses"] = sorted([
                        AvailableWarehouse(**w, distance_to_client=self.__count_distance_to_client(lat, lon, w['latitude'], w['longitude']))
                        for w in wh_valid
                    ], key=lambda x: (x.distance_to_client is None, x.distance_to_client or 0))
                else:
                    product_dict["available_warehouses"] = None
            else:
                product_dict["available_warehouses"] = None

            # Остальные поля
            product_dict["tags"] = product_dict.get("tags") or []
            product_dict["is_ad_pos"] = False
            product_dict["variations"] = []
            product_dict["stock_quantity"] = 0.0
            product_dict["distance"] = float(product_dict.get("distance")) if product_dict.get("distance") else None
            product_dict["seller_photo"] = None
            product_dict["cashbox_id"] = product_dict["cashbox"]

            products.append(MarketplaceProduct(**product_dict))

        return MarketplaceProductList(
            result=products,
            count=total_count,
            page=page,
            size=size
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
            result.append(AvailableWarehouse(**w, distance_to_client=self.__count_distance_to_client(client_lat, client_lon, w['latitude'], w['longitude'])))

        # Сортируем: сначала склады с известным расстоянием (по возрастанию), потом — без координат
        result.sort(key=lambda x: (x.distance_to_client is None, x.distance_to_client or 0))
        return result

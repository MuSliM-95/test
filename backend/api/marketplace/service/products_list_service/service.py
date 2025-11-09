from typing import Optional, List

from sqlalchemy import and_, select, func, asc, desc, literal_column, JSON, cast

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.products_list_service.schemas import MarketplaceProduct, MarketplaceProductList, \
    AvailableWarehouse
from database.db import nomenclature, prices, price_types, database, warehouses, warehouse_balances, units, categories, \
    manufacturers, cboxes, marketplace_rating_aggregates, pictures, nomenclature_barcodes


class MarketplaceProductsListService(BaseMarketplaceService):
    @staticmethod
    async def get_products(
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
        # Алиас для складов, связанных с балансами (чтобы не конфликтовать с основным warehouses)
        wh_bal = warehouses.alias("wh_bal")

        json_obj = func.json_build_object(
            literal_column("'warehouse_id'"), wh_bal.c.id,
            literal_column("'organization_id'"), warehouse_balances.c.organization_id,
            literal_column("'warehouse_name'"), wh_bal.c.name,
            literal_column("'warehouse_address'"), wh_bal.c.address
        )

        available_warehouses_agg = (
            func.array_agg(cast(json_obj, JSON))
            .filter(wh_bal.c.id.is_not(None))
            .label("available_warehouses")
        )

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
                prices.c.price,
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
            .join(prices, prices.c.nomenclature == nomenclature.c.id)
            .join(price_types, price_types.c.id == prices.c.price_type)
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
                warehouse_balances.c.current_amount > 0
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
            prices.c.is_deleted.is_not(True),
            price_types.c.name == "chatting",
        ]

        if category_filter:
            conditions.append(categories.c.name.ilike(f"%{category_filter}%"))
        if manufacturer_filter:
            conditions.append(manufacturers.c.name.ilike(f"%{manufacturer_filter}%"))
        if min_price is not None:
            conditions.append(prices.c.price >= min_price)
        if max_price is not None:
            conditions.append(prices.c.price <= max_price)

        query = query.where(and_(*conditions))

        group_by_fields = [
            nomenclature.c.id,
            units.c.convent_national_view,
            categories.c.name,
            manufacturers.c.name,
            prices.c.price,
            price_types.c.name,
            cboxes.c.name,
            marketplace_rating_aggregates.c.avg_rating,
            marketplace_rating_aggregates.c.reviews_count,
        ]
        query = query.group_by(*group_by_fields)

        if sort == "price":
            query = query.order_by(asc(prices.c.price))
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
        count_query = (
            select(func.count(nomenclature.c.id))
            .select_from(nomenclature)
            .join(prices, prices.c.nomenclature == nomenclature.c.id)
            .join(price_types, price_types.c.id == prices.c.price_type)
            .where(and_(*conditions))
        )
        count_result = await database.fetch_one(count_query)
        total_count = count_result[0] if count_result else 0

        products: List[MarketplaceProduct] = []
        for index, product in enumerate(products_db):
            product_dict = dict(product)
            product_dict["listing_pos"] = (page - 1) * size + index + 1
            product_dict["listing_page"] = page
            if product_dict.get("geo_point"):
                product_dict["geo_point"] = str(product_dict["geo_point"])

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
                    product_dict["available_warehouses"] = [
                        AvailableWarehouse(
                            warehouse_id=w["warehouse_id"],
                            organization_id=w["organization_id"],
                            warehouse_name=w["warehouse_name"],
                            warehouse_address=w["warehouse_address"]
                        )
                        for w in wh_valid
                    ]
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

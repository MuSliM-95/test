import hashlib
import json
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List

from fastapi import HTTPException
from sqlalchemy import select, func, and_, desc, asc, text, JSON, cast, literal_column

from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.utils.ioc.ioc import ioc
from database.db import nomenclature, units, categories, manufacturers, prices, price_types, cboxes, pictures, \
    nomenclature_barcodes, database, qr_codes, view_events, warehouses_rating_aggregates, warehouses, warehouse_balances
from . import schemas
from .rabbitmq.messages.CreateMarketplaceOrderMessage import CreateMarketplaceOrderMessage
from .schemas import MarketplaceOrderGood


class MarketplaceService:
    def __init__(self):
        self.__rabbitmq: Optional[IRabbitMessaging] = None

    async def connect(self):
        self.__rabbitmq = await ioc.get(IRabbitFactory)()

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
    ) -> schemas.MarketplaceProductList:
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
                warehouses_rating_aggregates.c.avg_rating.label("rating"),
                warehouses_rating_aggregates.c.reviews_count.label("reviews_count"),
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
            .join(warehouses_rating_aggregates, warehouses_rating_aggregates.c.warehouse_id == warehouses.c.id,
                  isouter=True)
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
            warehouses_rating_aggregates.c.avg_rating,
            warehouses_rating_aggregates.c.reviews_count,
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

        products: List[schemas.MarketplaceProduct] = []
        for index, product in enumerate(products_db):
            product_dict = dict(product)
            product_dict["listing_pos"] = (page - 1) * size + index + 1
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
                        schemas.AvailableWarehouse(
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

            products.append(schemas.MarketplaceProduct(**product_dict))

        return schemas.MarketplaceProductList(
            result=products,
            count=total_count,
            page=page,
            size=size
        )

    @staticmethod
    async def get_locations( # TODO: rewrite locations на warehouses balaneces
        city: Optional[str],
        lat: Optional[float],
        lon: Optional[float],
        radius: Optional[float],
        page: int,
        size: int,
        sort: Optional[str],
    ):
        offset = (page - 1) * size
        query = select(
            warehouses.c.id,
            warehouses.c.name,
            warehouses.c.address,
            warehouses.c.cashbox,
            warehouses.c.owner,
            warehouses.c.created_at,
            warehouses.c.updated_at,
        ).select_from(warehouses)

        conditions = [warehouses.c.is_public == True]
        # if city:
        #     conditions.append(cboxes.c.city.ilike(f"%{city}%"))
        query = query.where(and_(*conditions))

        if sort == "name":
            query = query.order_by(warehouses.c.name)
        else:
            query = query.order_by(warehouses.c.name)

        count_query = select(func.count()).select_from(warehouses).where(and_(*conditions))
        total_count = await database.fetch_val(count_query)

        query = query.limit(size).offset(offset)
        locations = await database.fetch_all(query)

        result: List[Dict[str, Any]] = []
        for location in locations:
            ld = dict(location)
            if ld.get("address"):
                ld["address"] = str(ld["address"])
            ld["cashbox_id"] = ld.get("cashbox")
            ld["admin_id"] = ld.get("owner")
            ld["avg_rating"] = None # TODO: add rating
            ld["reviews_count"] = 0
            result.append(ld)

        return {"result": result, "count": total_count, "page": page, "size": size}


    async def create_order(self, order_request: schemas.MarketplaceOrderRequest) -> schemas.MarketplaceOrderResponse:
        # группируем товары по cashbox
        goods_dict: dict[int, list[MarketplaceOrderGood]] = {}

        for good in order_request.goods:
            if goods_dict.get(good.cashbox_id):
                goods_dict[good.cashbox_id].append(good)
            else:
                goods_dict[good.cashbox_id] = [good]

        for cashbox, goods in goods_dict.items():
            await self.__rabbitmq.publish(
                CreateMarketplaceOrderMessage(
                    message_id=uuid.uuid4(),
                    cashbox_id=cashbox,
                    contragent_id=order_request.contragent_id,
                    goods=goods,
                    delivery_info=order_request.delivery,
                ),
                routing_key='create_marketplace_order',
            )

        return schemas.MarketplaceOrderResponse(
            message="Заказ создан и отправлен на обработку"
        )


async def resolve_qr(qr_hash: str) -> schemas.QRResolveResponse:
    qr_query = select(qr_codes.c.entity_type, qr_codes.c.entity_id, qr_codes.c.is_active).where(
        and_(qr_codes.c.qr_hash == qr_hash, qr_codes.c.is_active == True)
    )
    qr_record = await database.fetch_one(qr_query)
    if not qr_record:
        raise HTTPException(status_code=404, detail="QR-код не найден или неактивен")

    entity_type = qr_record.entity_type
    entity_id = qr_record.entity_id

    if entity_type == "product":
        product_query = select(
            nomenclature.c.id,
            nomenclature.c.name,
            nomenclature.c.description_short,
            nomenclature.c.description_long,
            nomenclature.c.code,
            nomenclature.c.geo_point,
            nomenclature.c.city,
            nomenclature.c.cashbox,
            nomenclature.c.public,
        ).where(and_(nomenclature.c.id == entity_id, nomenclature.c.public == True, nomenclature.c.is_deleted == False))
        product = await database.fetch_one(product_query)
        if not product:
            raise HTTPException(status_code=404, detail="Товар не найден или не доступен")

        price_query = (
            select(prices.c.price, price_types.c.name.label("price_type"))
            .select_from(prices.join(price_types, price_types.c.id == prices.c.price_type))
            .where(and_(prices.c.nomenclature == entity_id, price_types.c.name == "chatting"))
        )
        price_data = await database.fetch_one(price_query)
        entity_data = {
            "id": product.id,
            "name": product.name,
            "description_short": product.description_short,
            "description_long": product.description_long,
            "code": product.code,
            "unit_name": "шт",
            "category_name": "Категория",
            "manufacturer_name": "Производитель",
            "price": float(price_data.price) if price_data and price_data.price else 0.0,
            "price_type": price_data.price_type if price_data else "chatting",
            "geo_point": str(product.geo_point) if product.geo_point else None,
            "city": product.city,
            "cashbox_id": product.cashbox,
            "images": [],
            "barcodes": [],
        }
    elif entity_type == "location":
        location_query = select(
            cboxes.c.id,
            cboxes.c.name,
            cboxes.c.geo_point,
            cboxes.c.city,
            cboxes.c.admin,
            cboxes.c.public,
            cboxes.c.created_at,
            cboxes.c.updated_at,
        ).where(and_(cboxes.c.id == entity_id, cboxes.c.public == True))
        location = await database.fetch_one(location_query)
        if not location:
            raise HTTPException(status_code=404, detail="Локация не найдена или не доступна")

        entity_data = {
            "id": location.id,
            "name": location.name,
            "geo_point": str(location.geo_point) if location.geo_point else None,
            "city": location.city,
            "cashbox_id": location.id,
            "admin_id": location.admin,
            "created_at": location.created_at,
            "updated_at": location.updated_at,
            "avg_rating": None,
            "reviews_count": 0,
            "cashboxes": [],
        }
    else:
        raise HTTPException(status_code=400, detail="Неизвестный тип сущности")

    return schemas.QRResolveResponse(type=entity_type, entity=entity_data, qr_hash=qr_hash, resolved_at=datetime.now())


async def create_review(location_id: int, review_request: schemas.ReviewRequest) -> schemas.ReviewResponse:
    location_query = select(cboxes.c.id, cboxes.c.public).where(and_(cboxes.c.id == location_id, cboxes.c.public == True))
    location = await database.fetch_one(location_query)
    if not location:
        raise HTTPException(status_code=404, detail="Локация не найдена или не доступна")

    if not (1 <= review_request.rating <= 5):
        raise HTTPException(status_code=400, detail="Рейтинг должен быть от 1 до 5")
    if len(review_request.text.strip()) < 10:
        raise HTTPException(status_code=400, detail="Текст отзыва должен содержать минимум 10 символов")
    if len(review_request.text) > 1000:
        raise HTTPException(status_code=400, detail="Текст отзыва не должен превышать 1000 символов")

    phone_hash = hashlib.sha256(review_request.phone.encode()).hexdigest()
    recent_review_query = select(reviews.c.id).where(
        and_(reviews.c.location_id == location_id, reviews.c.phone_hash == phone_hash, reviews.c.created_at >= func.now() - text("INTERVAL '24 hours'"))
    )
    recent_review = await database.fetch_one(recent_review_query)
    if recent_review:
        raise HTTPException(status_code=429, detail="Можно оставить только один отзыв в сутки")

    review_data = {
        "location_id": location_id,
        "phone_hash": phone_hash,
        "rating": review_request.rating,
        "text": review_request.text.strip(),
        "status": "pending",
        "utm": review_request.utm,
    }
    review_id = await database.execute(reviews.insert().values(review_data))

    created_review_query = select(
        reviews.c.id,
        reviews.c.location_id,
        reviews.c.rating,
        reviews.c.text,
        reviews.c.phone_hash,
        reviews.c.status,
        reviews.c.created_at,
        reviews.c.utm,
    ).where(reviews.c.id == review_id)
    created_review = await database.fetch_one(created_review_query)

    utm_data = None
    if created_review.utm:
        try:
            if isinstance(created_review.utm, str):
                utm_data = json.loads(created_review.utm)
            else:
                utm_data = created_review.utm
        except Exception:
            utm_data = None

    return schemas.ReviewResponse(
        id=created_review.id,
        location_id=created_review.location_id,
        rating=created_review.rating,
        text=created_review.text,
        phone_hash=created_review.phone_hash,
        status=created_review.status,
        created_at=created_review.created_at,
        utm=utm_data,
    )


async def get_reviews(location_id: int, page: int, size: int, sort: Optional[str]) -> schemas.ReviewListResponse:
    location_query = select(cboxes.c.id, cboxes.c.public).where(and_(cboxes.c.id == location_id, cboxes.c.public == True))
    location = await database.fetch_one(location_query)
    if not location:
        raise HTTPException(status_code=404, detail="Локация не найдена или не доступна")

    offset = (page - 1) * size
    query = select(
        reviews.c.id,
        reviews.c.location_id,
        reviews.c.rating,
        reviews.c.text,
        reviews.c.phone_hash,
        reviews.c.status,
        reviews.c.created_at,
        reviews.c.utm,
    ).where(and_(reviews.c.location_id == location_id, reviews.c.status == "visible"))

    if sort == "newest":
        query = query.order_by(desc(reviews.c.created_at))
    elif sort == "oldest":
        query = query.order_by(asc(reviews.c.created_at))
    elif sort == "highest":
        query = query.order_by(desc(reviews.c.rating), desc(reviews.c.created_at))
    elif sort == "lowest":
        query = query.order_by(asc(reviews.c.rating), desc(reviews.c.created_at))
    else:
        query = query.order_by(desc(reviews.c.created_at))

    count_query = select(func.count()).where(and_(reviews.c.location_id == location_id, reviews.c.status == "visible"))
    total_count = await database.fetch_val(count_query)

    query = query.offset(offset).limit(size)
    reviews_data = await database.fetch_all(query)

    avg_rating_query = select(func.avg(reviews.c.rating)).where(and_(reviews.c.location_id == location_id, reviews.c.status == "visible"))
    avg_rating = await database.fetch_val(avg_rating_query)

    result: List[schemas.ReviewResponse] = []
    for review in reviews_data:
        utm_data = None
        if review.utm:
            try:
                if isinstance(review.utm, str):
                    utm_data = json.loads(review.utm)
                else:
                    utm_data = review.utm
            except Exception:
                utm_data = None
        result.append(
            schemas.ReviewResponse(
                id=review.id,
                location_id=review.location_id,
                rating=review.rating,
                text=review.text,
                phone_hash=review.phone_hash,
                status=review.status,
                created_at=review.created_at,
                utm=utm_data,
            )
        )

    return schemas.ReviewListResponse(
        result=result,
        count=total_count,
        page=page,
        size=size,
        avg_rating=float(avg_rating) if avg_rating else None,
    )


async def add_to_favorites(favorite_request: schemas.FavoriteRequest) -> schemas.FavoriteResponse:
    if favorite_request.entity_type != "product":
        raise HTTPException(status_code=400, detail="Тип сущности должен быть 'product'")

    product_query = select(nomenclature.c.id).where(
        and_(
            nomenclature.c.id == favorite_request.entity_id,
            nomenclature.c.public == True,
            nomenclature.c.is_deleted == False,
        )
    )
    entity = await database.fetch_one(product_query)
    if not entity:
        raise HTTPException(status_code=404, detail="Товар не найден или не доступен")

    phone_hash = hashlib.sha256(favorite_request.phone.encode()).hexdigest()
    existing_query = select(favorites.c.id).where(
        and_(
            favorites.c.phone_hash == phone_hash,
            favorites.c.entity_type == favorite_request.entity_type,
            favorites.c.entity_id == favorite_request.entity_id,
        )
    )
    existing_favorite = await database.fetch_one(existing_query)
    if existing_favorite:
        raise HTTPException(status_code=409, detail="Элемент уже добавлен в избранное")

    favorite_data = {
        "entity_type": favorite_request.entity_type,
        "entity_id": favorite_request.entity_id,
        "phone_hash": phone_hash,
        "utm": favorite_request.utm,
    }
    favorite_id = await database.execute(favorites.insert().values(favorite_data))

    created_favorite_query = select(
        favorites.c.id,
        favorites.c.entity_type,
        favorites.c.entity_id,
        favorites.c.phone_hash,
        favorites.c.created_at,
        favorites.c.utm,
    ).where(favorites.c.id == favorite_id)
    created_favorite = await database.fetch_one(created_favorite_query)

    utm_data = None
    if created_favorite.utm:
        try:
            if isinstance(created_favorite.utm, str):
                utm_data = json.loads(created_favorite.utm)
            else:
                utm_data = created_favorite.utm
        except Exception:
            utm_data = None

    return schemas.FavoriteResponse(
        id=created_favorite.id,
        entity_type=created_favorite.entity_type,
        entity_id=created_favorite.entity_id,
        phone_hash=created_favorite.phone_hash,
        created_at=created_favorite.created_at,
        utm=utm_data,
    )


async def remove_from_favorites(favorite_id: int, phone: str) -> Dict[str, Any]:
    phone_hash = hashlib.sha256(phone.encode()).hexdigest()
    favorite_query = select(favorites.c.id).where(
        and_(favorites.c.id == favorite_id, favorites.c.phone_hash == phone_hash)
    )
    favorite = await database.fetch_one(favorite_query)
    if not favorite:
        raise HTTPException(status_code=404, detail="Элемент не найден в избранном")
    await database.execute(favorites.delete().where(favorites.c.id == favorite_id))
    return {"message": "Элемент удален из избранного"}


async def get_favorites(
    phone: str, page: int, size: int, entity_type: Optional[str]
) -> schemas.FavoriteListResponse:
    phone_hash = hashlib.sha256(phone.encode()).hexdigest()
    offset = (page - 1) * size

    query = select(
        favorites.c.id,
        favorites.c.entity_type,
        favorites.c.entity_id,
        favorites.c.phone_hash,
        favorites.c.created_at,
        favorites.c.utm,
    ).where(favorites.c.phone_hash == phone_hash)

    if entity_type:
        if entity_type not in ["product", "location"]:
            raise HTTPException(status_code=400, detail="Тип сущности должен быть 'product' или 'location'")
        query = query.where(favorites.c.entity_type == entity_type)

    query = query.order_by(desc(favorites.c.created_at))
    count_query = select(func.count()).where(favorites.c.phone_hash == phone_hash)
    if entity_type:
        count_query = count_query.where(favorites.c.entity_type == entity_type)
    total_count = await database.fetch_val(count_query)

    query = query.offset(offset).limit(size)
    favorites_data = await database.fetch_all(query)

    result: List[schemas.FavoriteResponse] = []
    for favorite in favorites_data:
        utm_data = None
        if favorite.utm:
            try:
                if isinstance(favorite.utm, str):
                    utm_data = json.loads(favorite.utm)
                else:
                    utm_data = favorite.utm
            except Exception:
                utm_data = None
        result.append(
            schemas.FavoriteResponse(
                id=favorite.id,
                entity_type=favorite.entity_type,
                entity_id=favorite.entity_id,
                phone_hash=favorite.phone_hash,
                created_at=favorite.created_at,
                utm=utm_data,
            )
        )

    return schemas.FavoriteListResponse(result=result, count=total_count, page=page, size=size)


async def create_view_event(request: schemas.ViewEventRequest) -> schemas.ViewEventResponse:
    phone_hash = None
    if request.phone:
        phone_hash = hashlib.sha256(request.phone.encode()).hexdigest()
    query = view_events.insert().values(
        entity_type=request.entity_type,
        entity_id=request.entity_id,
        listing_pos=request.listing_pos,
        listing_page=request.listing_page,
        phone_hash=phone_hash,
        utm=request.utm,
    )
    await database.execute(query)
    return schemas.ViewEventResponse(success=True, message="Событие просмотра успешно сохранено")



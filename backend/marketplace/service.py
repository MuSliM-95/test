from typing import Optional, Dict, Any, List

from fastapi import HTTPException
from sqlalchemy import select, func, and_, or_, desc, asc, text
from datetime import datetime
import uuid
import json
import hashlib

from . import schemas
from .models import (
    database, nomenclature, prices, price_types, units, categories,
    manufacturers, pictures, nomenclature_barcodes, cboxes, mp_orders, qr_codes,
    reviews, location_rating_aggregates, favorites, view_events, warehouse_balances, tags
)
from producer import queue_marketplace_order


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
    query = (
        select(
            nomenclature.c.id,
            nomenclature.c.name,
            nomenclature.c.description_short,
            nomenclature.c.description_long,
            nomenclature.c.code,
            nomenclature.c.geo_point,
            nomenclature.c.city,
            nomenclature.c.created_at,
            nomenclature.c.updated_at,
            units.c.convent_national_view.label("unit_name"),
            categories.c.name.label("category_name"),
            manufacturers.c.name.label("manufacturer_name"),
            prices.c.price,
            price_types.c.name.label("price_type"),
            cboxes.c.name.label("seller_name"),
            location_rating_aggregates.c.avg_rating.label("rating"),
            location_rating_aggregates.c.reviews_count.label("reviews_count"),
            func.array_agg(func.distinct(pictures.c.url)).filter(pictures.c.url.is_not(None)).label("images"),
            func.array_agg(func.distinct(nomenclature_barcodes.c.code)).filter(nomenclature_barcodes.c.code.is_not(None)).label("barcodes"),
        )
        .select_from(nomenclature)
        .join(units, units.c.id == nomenclature.c.unit, full=True)
        .join(categories, categories.c.id == nomenclature.c.category, full=True)
        .join(manufacturers, manufacturers.c.id == nomenclature.c.manufacturer, full=True)
        .join(prices, prices.c.nomenclature == nomenclature.c.id, full=True)
        .join(price_types, price_types.c.id == prices.c.price_type, full=True)
        .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, full=True)
        .join(pictures, and_(
            pictures.c.entity == "nomenclature",
            pictures.c.entity_id == nomenclature.c.id,
            pictures.c.is_deleted.is_not(True)
        ), full=True)
        .join(nomenclature_barcodes, nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id, full=True)
        .join(location_rating_aggregates, location_rating_aggregates.c.location_id == cboxes.c.id, full=True)
    )

    conditions = [
        nomenclature.c.public == True,
        nomenclature.c.is_deleted.is_not(True),
        prices.c.is_deleted.is_not(True),
        price_types.c.name == "chatting",
    ]

    if city:
        conditions.append(or_(nomenclature.c.city.ilike(f"%{city}%"), cboxes.c.city.ilike(f"%{city}%")))
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
        location_rating_aggregates.c.avg_rating,
        location_rating_aggregates.c.reviews_count,
    ]
    query = query.group_by(*group_by_fields)

    if sort == "price":
        query = query.order_by(asc(prices.c.price))
    elif sort == "name":
        query = query.order_by(asc(nomenclature.c.name))
    elif sort == "created_at":
        query = query.order_by(desc(nomenclature.c.created_at))
    elif sort == "rating":
        query = query.order_by(desc(location_rating_aggregates.c.avg_rating))
    else:
        query = query.order_by(desc(nomenclature.c.id))

    offset = (page - 1) * size
    query = query.limit(size).offset(offset)

    products_db = await database.fetch_all(query)

    count_query = (
        select(func.count(nomenclature.c.id))
        .select_from(nomenclature)
        .join(prices, prices.c.nomenclature == nomenclature.c.id)
        .join(price_types, price_types.c.id == prices.c.price_type)
        .where(and_(*conditions))
    )
    if city:
        count_query = count_query.join(cboxes, cboxes.c.id == nomenclature.c.cashbox)

    count_result = await database.fetch_one(count_query)
    total_count = count_result[0] if count_result else 0

    products: List[schemas.MarketplaceProduct] = []
    for index, product in enumerate(products_db):
        product_dict = dict(product)
        product_dict["listing_pos"] = (page - 1) * size + index + 1
        if product_dict.get("geo_point"):
            product_dict["geo_point"] = str(product_dict["geo_point"])
        product_dict["images"] = product_dict.get("images") or []
        product_dict["barcodes"] = product_dict.get("barcodes") or []
        product_dict["tags"] = product_dict.get("tags") or []
        product_dict["is_ad_pos"] = False
        product_dict["variations"] = []
        product_dict["stock_quantity"] = 0.0
        product_dict["rating"] = float(product_dict.get("rating")) if product_dict.get("rating") else None
        product_dict["reviews_count"] = product_dict.get("reviews_count", 0)
        product_dict["distance"] = float(product_dict.get("distance")) if product_dict.get("distance") else None
        product_dict["seller_photo"] = None
        products.append(schemas.MarketplaceProduct(**product_dict))

    return schemas.MarketplaceProductList(result=products, count=total_count, page=page, size=size)


async def get_locations(
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
        cboxes.c.id,
        cboxes.c.name,
        cboxes.c.geo_point,
        cboxes.c.city,
        cboxes.c.admin,
        cboxes.c.created_at,
        cboxes.c.updated_at,
    ).select_from(cboxes)

    conditions = [cboxes.c.public == True]
    if city:
        conditions.append(cboxes.c.city.ilike(f"%{city}%"))
    query = query.where(and_(*conditions))

    if sort == "name":
        query = query.order_by(cboxes.c.name)
    else:
        query = query.order_by(cboxes.c.name)

    count_query = select(func.count()).select_from(cboxes).where(and_(*conditions))
    total_count = await database.fetch_val(count_query)

    query = query.offset(offset).limit(size)
    locations = await database.fetch_all(query)

    result: List[Dict[str, Any]] = []
    for location in locations:
        ld = dict(location)
        if ld.get("geo_point"):
            ld["geo_point"] = str(ld["geo_point"])
        ld["cashbox_id"] = ld.get("id")
        ld["admin_id"] = ld.get("admin")
        ld["avg_rating"] = None
        ld["reviews_count"] = 0
        ld["cashboxes"] = []
        result.append(ld)

    return {"result": result, "count": total_count, "page": page, "size": size}


async def create_order(order_request: schemas.MarketplaceOrderRequest) -> schemas.MarketplaceOrderResponse:
    order_id = f"mp_{uuid.uuid4().hex[:12]}"

    product_query = select(
        nomenclature.c.id,
        nomenclature.c.name,
        nomenclature.c.cashbox,
        nomenclature.c.public,
    ).where(
        and_(
            nomenclature.c.id == order_request.product_id,
            nomenclature.c.public == True,
            nomenclature.c.is_deleted == False,
        )
    )
    product = await database.fetch_one(product_query)
    if not product:
        raise HTTPException(status_code=404, detail="Товар не найден или не доступен")

    order_data = {
        "order_id": order_id,
        "product_id": order_request.product_id,
        "listing_pos": order_request.listing_pos,
        "listing_page": order_request.listing_page,
        "location_id": order_request.location_id,
        "utm": order_request.utm,
        "delivery_type": order_request.delivery.type,
        "delivery_address": order_request.delivery.address,
        "delivery_comment": order_request.delivery.comment,
        "delivery_preferred_time": order_request.delivery.preferred_time,
        "customer_phone": order_request.customer.phone,
        "customer_lat": order_request.customer.lat,
        "customer_lon": order_request.customer.lon,
        "customer_name": order_request.customer.name,
        "quantity": order_request.quantity,
        "status": "pending",
        "routing_meta": {
            "product_cashbox": product.cashbox,
            "product_name": product.name,
            "distribution_strategy": "nearest_viable_with_stock",
        },
    }

    await database.execute(mp_orders.insert().values(order_data))

    rabbitmq_data = {
        "order_id": order_id,
        "product_id": order_request.product_id,
        "product_cashbox": product.cashbox,
        "quantity": order_request.quantity,
        "customer_phone": order_request.customer.phone,
        "delivery_type": order_request.delivery.type,
        "delivery_address": order_request.delivery.address,
        "customer_lat": order_request.customer.lat,
        "customer_lon": order_request.customer.lon,
    }
    queue_success = await queue_marketplace_order(rabbitmq_data)
    if not queue_success:
        await database.execute(
            mp_orders.update()
            .where(mp_orders.c.order_id == order_id)
            .values(status="failed", routing_meta={"error": "Failed to queue order"})
        )
        raise HTTPException(status_code=500, detail="Ошибка при обработке заказа")

    return schemas.MarketplaceOrderResponse(
        order_id=order_id,
        status="pending",
        message="Заказ создан и отправлен на обработку",
        estimated_delivery="30-60 минут",
        cashbox_assignments=[
            {
                "cashbox_id": product.cashbox,
                "product_name": product.name,
                "quantity": order_request.quantity,
                "status": "assigned",
            }
        ],
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



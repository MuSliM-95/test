from typing import Optional
from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import select, func, and_, or_, desc, asc, text
from sqlalchemy.sql.functions import coalesce
import uuid
import json
import hashlib
from datetime import datetime

import api.marketplace.schemas as schemas
from database.db import (
    database, nomenclature, prices, price_types, units, categories, 
    manufacturers, pictures, nomenclature_barcodes, cboxes, mp_orders, qr_codes,
    reviews, location_rating_aggregates, favorites, view_events, warehouse_balances, tags
)
from producer import queue_marketplace_order

router = APIRouter(prefix="/mp", tags=["marketplace"])


@router.get("/products", response_model=schemas.MarketplaceProductList)
async def get_marketplace_products(
    phone: Optional[str] = Query(None, description="Телефон клиента"),
    lat: Optional[float] = Query(None, description="Широта"),
    lon: Optional[float] = Query(None, description="Долгота"),
    city: Optional[str] = Query(None, description="Город"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    size: int = Query(20, ge=1, le=100, description="Размер страницы"),
    sort: Optional[str] = Query(None, description="Сортировка: distance, price, name, created_at, rating"),
    category: Optional[str] = Query(None, description="Фильтр по категории"),
    manufacturer: Optional[str] = Query(None, description="Фильтр по производителю"),
    min_price: Optional[float] = Query(None, description="Минимальная цена"),
    max_price: Optional[float] = Query(None, description="Максимальная цена"),
    tags: Optional[str] = Query(None, description="Фильтр по тегам (через запятую)"),
    in_stock: Optional[bool] = Query(None, description="Только товары в наличии")
):
    """
    Получение всех публичных товаров маркетплейса
    
    Фильтрует только товары с:
    - public = true
    - price_type = 'chatting'
    - Поддерживает геолокацию, персонализацию и расширенные фильтры
    """
    
    # Базовый запрос с джойнами
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
            # Селлер (владелец кешбокса)
            cboxes.c.name.label("seller_name"),
            # cboxes.c.photo.label("seller_photo"),  # Поле photo отсутствует в таблице
            # Остатки (пока отключаем, добавим позже)
            # func.coalesce(func.sum(warehouse_balances.c.quantity), 0).label("stock_quantity"),
            # Рейтинг и отзывы (агрегированные данные)
            location_rating_aggregates.c.avg_rating.label("rating"),
            location_rating_aggregates.c.reviews_count.label("reviews_count"),
            # Изображения как массив URL
            func.array_agg(
                func.distinct(pictures.c.url)
            ).filter(pictures.c.url.is_not(None)).label("images"),
            # Штрихкоды как массив
            func.array_agg(
                func.distinct(nomenclature_barcodes.c.code)
            ).filter(nomenclature_barcodes.c.code.is_not(None)).label("barcodes"),
            # Теги (пока отключаем)
            # func.array_agg(
            #     func.distinct(tags.c.name)
            # ).filter(tags.c.name.is_not(None)).label("tags")
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
        # .join(warehouse_balances, and_(
        #     warehouse_balances.c.nomenclature_id == nomenclature.c.id,
        #     warehouse_balances.c.quantity > 0
        # ), full=True)
        .join(location_rating_aggregates, location_rating_aggregates.c.location_id == cboxes.c.id, full=True)
        # .join(tags, and_(
        #     tags.c.entity_type == "nomenclature",
        #     tags.c.entity_id == nomenclature.c.id
        # ), full=True)
    )
    
    # Условия фильтрации
    conditions = [
        nomenclature.c.public == True,  # Только публичные товары
        nomenclature.c.is_deleted.is_not(True),
        prices.c.is_deleted.is_not(True),
        price_types.c.name == "chatting"  # Только цены типа "chatting"
    ]
    
    # Фильтр по городу
    if city:
        conditions.append(
            or_(
                nomenclature.c.city.ilike(f"%{city}%"),
                cboxes.c.city.ilike(f"%{city}%")
            )
        )
    
    # Фильтр по категории
    if category:
        conditions.append(categories.c.name.ilike(f"%{category}%"))
    
    # Фильтр по производителю
    if manufacturer:
        conditions.append(manufacturers.c.name.ilike(f"%{manufacturer}%"))
    
    # Фильтр по цене
    if min_price is not None:
        conditions.append(prices.c.price >= min_price)
    if max_price is not None:
        conditions.append(prices.c.price <= max_price)
    
    # Фильтр по тегам (пока отключаем)
    # if tags:
    #     tag_list = [tag.strip() for tag in tags.split(",")]
    #     conditions.append(tags.c.name.in_(tag_list))
    
    # Фильтр по наличию (пока отключаем)
    # if in_stock:
    #     conditions.append(warehouse_balances.c.quantity > 0)
    
    # Геолокация и расчет расстояния (пока отключаем PostGIS)
    # if lat is not None and lon is not None:
    #     # Используем PostGIS функцию ST_Distance для расчета расстояния
    #     distance_expr = func.ST_Distance(
    #         func.ST_GeogFromText(f"POINT({lon} {lat})"),
    #         func.ST_GeogFromText(nomenclature.c.geo_point)
    #     )
    #     query = query.add_columns(distance_expr.label("distance"))
    
    query = query.where(and_(*conditions))
    
    # Группировка для агрегации изображений и штрихкодов
    group_by_fields = [
        nomenclature.c.id,
        units.c.convent_national_view,
        categories.c.name,
        manufacturers.c.name,
        prices.c.price,
        price_types.c.name,
        cboxes.c.name,
        # cboxes.c.photo,  # Поле photo отсутствует в таблице
        location_rating_aggregates.c.avg_rating,
        location_rating_aggregates.c.reviews_count
    ]
    
    # Добавляем distance в группировку если есть геолокация (пока отключаем)
    # if lat is not None and lon is not None:
    #     group_by_fields.append(distance_expr)
    
    query = query.group_by(*group_by_fields)
    
    # Сортировка
    if sort == "distance" and lat is not None and lon is not None:
        query = query.order_by(asc("distance"))
    elif sort == "price":
        query = query.order_by(asc(prices.c.price))
    elif sort == "name":
        query = query.order_by(asc(nomenclature.c.name))
    elif sort == "created_at":
        query = query.order_by(desc(nomenclature.c.created_at))
    elif sort == "rating":
        query = query.order_by(desc(location_rating_aggregates.c.avg_rating))
    else:
        # По умолчанию сортируем по ID
        query = query.order_by(desc(nomenclature.c.id))
    
    # Пагинация
    offset = (page - 1) * size
    query = query.limit(size).offset(offset)
    
    # Выполняем запрос
    products_db = await database.fetch_all(query)
    
    # Подсчет общего количества
    count_query = (
        select(func.count(nomenclature.c.id))
        .select_from(nomenclature)
        .join(prices, prices.c.nomenclature == nomenclature.c.id)
        .join(price_types, price_types.c.id == prices.c.price_type)
    )
    
    if city:
        count_query = count_query.join(cboxes, cboxes.c.id == nomenclature.c.cashbox)
    
    count_query = count_query.where(and_(*conditions))
    count_result = await database.fetch_one(count_query)
    total_count = count_result[0] if count_result else 0
    
    # Преобразуем результаты в нужный формат
    products = []
    for index, product in enumerate(products_db):
        product_dict = dict(product)
        
        # Позиция в выдаче для аналитики
        product_dict["listing_pos"] = (page - 1) * size + index + 1
        
        # Преобразуем geo_point в строку если есть
        if product_dict.get("geo_point"):
            product_dict["geo_point"] = str(product_dict["geo_point"])
        
        # Обрабатываем массивы
        product_dict["images"] = product_dict.get("images") or []
        product_dict["barcodes"] = product_dict.get("barcodes") or []
        product_dict["tags"] = product_dict.get("tags") or []
        
        # Новые поля
        product_dict["is_ad_pos"] = False  # TODO: Добавить логику рекламных размещений
        product_dict["variations"] = []  # TODO: Добавить вариации товара
        product_dict["stock_quantity"] = 0.0  # Пока отключаем остатки
        product_dict["rating"] = float(product_dict.get("rating")) if product_dict.get("rating") else None
        product_dict["reviews_count"] = product_dict.get("reviews_count", 0)
        product_dict["distance"] = float(product_dict.get("distance")) if product_dict.get("distance") else None
        product_dict["seller_photo"] = None  # Поле photo отсутствует в таблице
        
        products.append(schemas.MarketplaceProduct(**product_dict))
    
    return schemas.MarketplaceProductList(
        result=products,
        count=total_count,
        page=page,
        size=size
    )


@router.get("/locations", response_model=schemas.MarketplaceLocationList)
async def get_marketplace_locations(
    city: Optional[str] = Query(None, description="Город"),
    lat: Optional[float] = Query(None, description="Широта"),
    lon: Optional[float] = Query(None, description="Долгота"),
    radius: Optional[float] = Query(None, description="Радиус поиска в км"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    size: int = Query(20, ge=1, le=100, description="Размер страницы"),
    sort: Optional[str] = Query("name", description="Сортировка: name, distance, rating")
):
    """
    Получить список публичных локаций для чекина
    """
    offset = (page - 1) * size
    
    # Базовый запрос для публичных локаций
    query = select(
        cboxes.c.id,
        cboxes.c.name,
        cboxes.c.geo_point,
        cboxes.c.city,
        cboxes.c.admin,
        cboxes.c.created_at,
        cboxes.c.updated_at
    ).select_from(cboxes)
    
    # Условия фильтрации
    conditions = [
        cboxes.c.public == True
    ]
    
    # Фильтр по городу
    if city:
        conditions.append(
            cboxes.c.city.ilike(f"%{city}%")
        )
    
    # TODO: Добавить фильтрацию по радиусу и координатам
    # if lat is not None and lon is not None and radius:
    #     # Расчет расстояния и фильтрация по радиусу
    #     pass
    
    query = query.where(and_(*conditions))
    
    # Сортировка
    if sort == "name":
        query = query.order_by(cboxes.c.name)
    elif sort == "rating":
        # TODO: Добавить сортировку по рейтингу
        query = query.order_by(cboxes.c.name)
    elif sort == "distance":
        # TODO: Добавить сортировку по расстоянию
        query = query.order_by(cboxes.c.name)
    else:
        query = query.order_by(cboxes.c.name)
    
    # Подсчет общего количества
    count_query = select(func.count()).select_from(cboxes).where(and_(*conditions))
    total_count = await database.fetch_val(count_query)
    
    # Пагинация
    query = query.offset(offset).limit(size)
    
    # Выполнение запроса
    locations = await database.fetch_all(query)
    
    # Формирование результата
    result = []
    for location in locations:
        location_dict = dict(location)
        
        # Преобразуем geo_point в строку если есть
        if location_dict.get("geo_point"):
            location_dict["geo_point"] = str(location_dict["geo_point"])
        
        # Заполняем обязательные поля
        location_dict["cashbox_id"] = location_dict.get("id")
        location_dict["admin_id"] = location_dict.get("admin")
        
        # TODO: Добавить расчет рейтинга и количества отзывов
        location_dict["avg_rating"] = None
        location_dict["reviews_count"] = 0
        location_dict["cashboxes"] = []  # Связанные кешбоксы
        
        result.append(location_dict)
    
    return {
        "result": result,
        "count": total_count,
        "page": page,
        "size": size
    }


@router.post("/orders", response_model=schemas.MarketplaceOrderResponse)
async def create_marketplace_order(order_request: schemas.MarketplaceOrderRequest):
    """
    Создать заказ маркетплейса с автоматическим распределением по кешбоксам
    """
    try:
        # Генерируем уникальный ID заказа
        order_id = f"mp_{uuid.uuid4().hex[:12]}"
        
        # Проверяем, что товар существует и публичный
        product_query = select(
            nomenclature.c.id,
            nomenclature.c.name,
            nomenclature.c.cashbox,
            nomenclature.c.public
        ).where(
            and_(
                nomenclature.c.id == order_request.product_id,
                nomenclature.c.public == True,
                nomenclature.c.is_deleted == False
            )
        )
        
        product = await database.fetch_one(product_query)
        if not product:
            raise HTTPException(status_code=404, detail="Товар не найден или не доступен")
        
        # Создаем запись заказа в базе данных
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
                "distribution_strategy": "nearest_viable_with_stock"
            }
        }
        
        # Сохраняем заказ в базе данных
        await database.execute(mp_orders.insert().values(order_data))
        
        # Отправляем заказ в RabbitMQ для распределения
        rabbitmq_data = {
            "order_id": order_id,
            "product_id": order_request.product_id,
            "product_cashbox": product.cashbox,
            "quantity": order_request.quantity,
            "customer_phone": order_request.customer.phone,
            "delivery_type": order_request.delivery.type,
            "delivery_address": order_request.delivery.address,
            "customer_lat": order_request.customer.lat,
            "customer_lon": order_request.customer.lon
        }
        
        # Отправляем в очередь
        queue_success = await queue_marketplace_order(rabbitmq_data)
        
        if not queue_success:
            # Если не удалось отправить в очередь, обновляем статус заказа
            await database.execute(
                mp_orders.update()
                .where(mp_orders.c.order_id == order_id)
                .values(status="failed", routing_meta={"error": "Failed to queue order"})
            )
            raise HTTPException(status_code=500, detail="Ошибка при обработке заказа")
        
        # Возвращаем ответ
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
                    "status": "assigned"
                }
            ]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Ошибка при создании заказа: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("/qr/{qr_hash}", response_model=schemas.QRResolveResponse)
async def resolve_qr_code(qr_hash: str):
    """
    Получить товар или локацию по QR-коду (MD5 хэш)
    """
    try:
        # Ищем QR-код в базе данных
        qr_query = select(
            qr_codes.c.entity_type,
            qr_codes.c.entity_id,
            qr_codes.c.is_active
        ).where(
            and_(
                qr_codes.c.qr_hash == qr_hash,
                qr_codes.c.is_active == True
            )
        )
        
        qr_record = await database.fetch_one(qr_query)
        if not qr_record:
            raise HTTPException(status_code=404, detail="QR-код не найден или неактивен")
        
        entity_type = qr_record.entity_type
        entity_id = qr_record.entity_id
        
        if entity_type == "product":
            # Получаем информацию о товаре (упрощенный запрос)
            product_query = select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.description_short,
                nomenclature.c.description_long,
                nomenclature.c.code,
                nomenclature.c.geo_point,
                nomenclature.c.city,
                nomenclature.c.cashbox,
                nomenclature.c.public
            ).where(
                and_(
                    nomenclature.c.id == entity_id,
                    nomenclature.c.public == True,
                    nomenclature.c.is_deleted == False
                )
            )
            
            product = await database.fetch_one(product_query)
            if not product:
                raise HTTPException(status_code=404, detail="Товар не найден или не доступен")
            
            # Получаем цену отдельно
            price_query = select(
                prices.c.price,
                price_types.c.name.label("price_type")
            ).select_from(
                prices.join(price_types, price_types.c.id == prices.c.price_type)
            ).where(
                and_(
                    prices.c.nomenclature == entity_id,
                    price_types.c.name == "chatting"
                )
            )
            
            price_data = await database.fetch_one(price_query)
            
            # Формируем данные товара
            entity_data = {
                "id": product.id,
                "name": product.name,
                "description_short": product.description_short,
                "description_long": product.description_long,
                "code": product.code,
                "unit_name": "шт",  # По умолчанию
                "category_name": "Категория",  # По умолчанию
                "manufacturer_name": "Производитель",  # По умолчанию
                "price": float(price_data.price) if price_data and price_data.price else 0.0,
                "price_type": price_data.price_type if price_data else "chatting",
                "geo_point": str(product.geo_point) if product.geo_point else None,
                "city": product.city,
                "cashbox_id": product.cashbox,
                "images": [],
                "barcodes": []
            }
            
        elif entity_type == "location":
            # Получаем информацию о локации
            location_query = select(
                cboxes.c.id,
                cboxes.c.name,
                cboxes.c.geo_point,
                cboxes.c.city,
                cboxes.c.admin,
                cboxes.c.public,
                cboxes.c.created_at,
                cboxes.c.updated_at
            ).where(
                and_(
                    cboxes.c.id == entity_id,
                    cboxes.c.public == True
                )
            )
            
            location = await database.fetch_one(location_query)
            if not location:
                raise HTTPException(status_code=404, detail="Локация не найдена или не доступна")
            
            # Формируем данные локации
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
                "cashboxes": []
            }
            
        else:
            raise HTTPException(status_code=400, detail="Неизвестный тип сущности")
        
        return schemas.QRResolveResponse(
            type=entity_type,
            entity=entity_data,
            qr_hash=qr_hash,
            resolved_at=datetime.now()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Ошибка при резолве QR-кода: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.post("/locations/{location_id}/reviews", response_model=schemas.ReviewResponse)
async def create_review(location_id: int, review_request: schemas.ReviewRequest):
    """
    Создать отзыв о локации
    """
    try:
        # Проверяем, что локация существует и публичная
        location_query = select(cboxes.c.id, cboxes.c.public).where(
            and_(
                cboxes.c.id == location_id,
                cboxes.c.public == True
            )
        )
        
        location = await database.fetch_one(location_query)
        if not location:
            raise HTTPException(status_code=404, detail="Локация не найдена или не доступна")
        
        # Валидация рейтинга
        if not (1 <= review_request.rating <= 5):
            raise HTTPException(status_code=400, detail="Рейтинг должен быть от 1 до 5")
        
        # Валидация текста
        if len(review_request.text.strip()) < 10:
            raise HTTPException(status_code=400, detail="Текст отзыва должен содержать минимум 10 символов")
        
        if len(review_request.text) > 1000:
            raise HTTPException(status_code=400, detail="Текст отзыва не должен превышать 1000 символов")
        
        # Хэшируем телефон для анонимности
        phone_hash = hashlib.sha256(review_request.phone.encode()).hexdigest()
        
        # Проверяем, не оставлял ли пользователь уже отзыв за последние 24 часа
        recent_review_query = select(reviews.c.id).where(
            and_(
                reviews.c.location_id == location_id,
                reviews.c.phone_hash == phone_hash,
                reviews.c.created_at >= func.now() - text("INTERVAL '24 hours'")
            )
        )
        
        recent_review = await database.fetch_one(recent_review_query)
        if recent_review:
            raise HTTPException(status_code=429, detail="Можно оставить только один отзыв в сутки")
        
        # Создаем отзыв
        review_data = {
            "location_id": location_id,
            "phone_hash": phone_hash,
            "rating": review_request.rating,
            "text": review_request.text.strip(),
            "status": "pending",  # На модерации
            "utm": review_request.utm
        }
        
        # Сохраняем отзыв
        review_id = await database.execute(reviews.insert().values(review_data))
        
        # Получаем созданный отзыв
        created_review_query = select(
            reviews.c.id,
            reviews.c.location_id,
            reviews.c.rating,
            reviews.c.text,
            reviews.c.phone_hash,
            reviews.c.status,
            reviews.c.created_at,
            reviews.c.utm
        ).where(reviews.c.id == review_id)
        
        created_review = await database.fetch_one(created_review_query)
        
        # Обрабатываем UTM данные
        utm_data = None
        if created_review.utm:
            try:
                if isinstance(created_review.utm, str):
                    utm_data = json.loads(created_review.utm)
                else:
                    utm_data = created_review.utm
            except:
                utm_data = None
        
        return schemas.ReviewResponse(
            id=created_review.id,
            location_id=created_review.location_id,
            rating=created_review.rating,
            text=created_review.text,
            phone_hash=created_review.phone_hash,
            status=created_review.status,
            created_at=created_review.created_at,
            utm=utm_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Ошибка при создании отзыва: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("/locations/{location_id}/reviews", response_model=schemas.ReviewListResponse)
async def get_reviews(
    location_id: int,
    page: int = Query(1, ge=1, description="Номер страницы"),
    size: int = Query(20, ge=1, le=100, description="Размер страницы"),
    sort: Optional[str] = Query("newest", description="Сортировка: newest, oldest, highest, lowest")
):
    """
    Получить отзывы о локации
    """
    try:
        # Проверяем, что локация существует и публичная
        location_query = select(cboxes.c.id, cboxes.c.public).where(
            and_(
                cboxes.c.id == location_id,
                cboxes.c.public == True
            )
        )
        
        location = await database.fetch_one(location_query)
        if not location:
            raise HTTPException(status_code=404, detail="Локация не найдена или не доступна")
        
        offset = (page - 1) * size
        
        # Базовый запрос для отзывов
        query = select(
            reviews.c.id,
            reviews.c.location_id,
            reviews.c.rating,
            reviews.c.text,
            reviews.c.phone_hash,
            reviews.c.status,
            reviews.c.created_at,
            reviews.c.utm
        ).where(
            and_(
                reviews.c.location_id == location_id,
                reviews.c.status == "visible"  # Только видимые отзывы
            )
        )
        
        # Сортировка
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
        
        # Подсчет общего количества видимых отзывов
        count_query = select(func.count()).where(
            and_(
                reviews.c.location_id == location_id,
                reviews.c.status == "visible"
            )
        )
        total_count = await database.fetch_val(count_query)
        
        # Пагинация
        query = query.offset(offset).limit(size)
        
        # Выполнение запроса
        reviews_data = await database.fetch_all(query)
        
        # Получаем средний рейтинг
        avg_rating_query = select(func.avg(reviews.c.rating)).where(
            and_(
                reviews.c.location_id == location_id,
                reviews.c.status == "visible"
            )
        )
        avg_rating = await database.fetch_val(avg_rating_query)
        
        # Формирование результата
        result = []
        for review in reviews_data:
            # Обрабатываем UTM данные
            utm_data = None
            if review.utm:
                try:
                    if isinstance(review.utm, str):
                        utm_data = json.loads(review.utm)
                    else:
                        utm_data = review.utm
                except:
                    utm_data = None
            
            result.append(schemas.ReviewResponse(
                id=review.id,
                location_id=review.location_id,
                rating=review.rating,
                text=review.text,
                phone_hash=review.phone_hash,
                status=review.status,
                created_at=review.created_at,
                utm=utm_data
            ))
        
        return schemas.ReviewListResponse(
            result=result,
            count=total_count,
            page=page,
            size=size,
            avg_rating=float(avg_rating) if avg_rating else None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Ошибка при получении отзывов: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.post("/favorites", response_model=schemas.FavoriteResponse)
async def add_to_favorites(favorite_request: schemas.FavoriteRequest):
    """
    Добавить товар или локацию в избранное
    """
    try:
        # Валидация типа сущности
        if favorite_request.entity_type not in ["product", "location"]:
            raise HTTPException(status_code=400, detail="Тип сущности должен быть 'product' или 'location'")
        
        # Проверяем существование сущности
        if favorite_request.entity_type == "product":
            # Проверяем, что товар существует и публичный
            product_query = select(nomenclature.c.id).where(
                and_(
                    nomenclature.c.id == favorite_request.entity_id,
                    nomenclature.c.public == True,
                    nomenclature.c.is_deleted == False
                )
            )
            entity = await database.fetch_one(product_query)
            if not entity:
                raise HTTPException(status_code=404, detail="Товар не найден или не доступен")
                
        elif favorite_request.entity_type == "location":
            # Проверяем, что локация существует и публичная
            location_query = select(cboxes.c.id).where(
                and_(
                    cboxes.c.id == favorite_request.entity_id,
                    cboxes.c.public == True
                )
            )
            entity = await database.fetch_one(location_query)
            if not entity:
                raise HTTPException(status_code=404, detail="Локация не найдена или не доступна")
        
        # Хэшируем телефон для анонимности
        phone_hash = hashlib.sha256(favorite_request.phone.encode()).hexdigest()
        
        # Проверяем, не добавлен ли уже этот элемент в избранное
        existing_query = select(favorites.c.id).where(
            and_(
                favorites.c.phone_hash == phone_hash,
                favorites.c.entity_type == favorite_request.entity_type,
                favorites.c.entity_id == favorite_request.entity_id
            )
        )
        
        existing_favorite = await database.fetch_one(existing_query)
        if existing_favorite:
            raise HTTPException(status_code=409, detail="Элемент уже добавлен в избранное")
        
        # Добавляем в избранное
        favorite_data = {
            "entity_type": favorite_request.entity_type,
            "entity_id": favorite_request.entity_id,
            "phone_hash": phone_hash,
            "utm": favorite_request.utm
        }
        
        # Сохраняем избранное
        favorite_id = await database.execute(favorites.insert().values(favorite_data))
        
        # Получаем созданное избранное
        created_favorite_query = select(
            favorites.c.id,
            favorites.c.entity_type,
            favorites.c.entity_id,
            favorites.c.phone_hash,
            favorites.c.created_at,
            favorites.c.utm
        ).where(favorites.c.id == favorite_id)
        
        created_favorite = await database.fetch_one(created_favorite_query)
        
        # Обрабатываем UTM данные
        utm_data = None
        if created_favorite.utm:
            try:
                if isinstance(created_favorite.utm, str):
                    utm_data = json.loads(created_favorite.utm)
                else:
                    utm_data = created_favorite.utm
            except:
                utm_data = None
        
        return schemas.FavoriteResponse(
            id=created_favorite.id,
            entity_type=created_favorite.entity_type,
            entity_id=created_favorite.entity_id,
            phone_hash=created_favorite.phone_hash,
            created_at=created_favorite.created_at,
            utm=utm_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Ошибка при добавлении в избранное: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.delete("/favorites/{favorite_id}")
async def remove_from_favorites(favorite_id: int, phone: str = Query(..., description="Номер телефона")):
    """
    Удалить элемент из избранного
    """
    try:
        # Хэшируем телефон для поиска
        phone_hash = hashlib.sha256(phone.encode()).hexdigest()
        
        # Проверяем, что избранное существует и принадлежит пользователю
        favorite_query = select(favorites.c.id).where(
            and_(
                favorites.c.id == favorite_id,
                favorites.c.phone_hash == phone_hash
            )
        )
        
        favorite = await database.fetch_one(favorite_query)
        if not favorite:
            raise HTTPException(status_code=404, detail="Элемент не найден в избранном")
        
        # Удаляем из избранного
        await database.execute(
            favorites.delete().where(favorites.c.id == favorite_id)
        )
        
        return {"message": "Элемент удален из избранного"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Ошибка при удалении из избранного: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("/favorites", response_model=schemas.FavoriteListResponse)
async def get_favorites(
    phone: str = Query(..., description="Номер телефона"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    size: int = Query(20, ge=1, le=100, description="Размер страницы"),
    entity_type: Optional[str] = Query(None, description="Фильтр по типу: product или location")
):
    """
    Получить список избранного пользователя
    """
    try:
        # Хэшируем телефон для поиска
        phone_hash = hashlib.sha256(phone.encode()).hexdigest()
        
        offset = (page - 1) * size
        
        # Базовый запрос для избранного
        query = select(
            favorites.c.id,
            favorites.c.entity_type,
            favorites.c.entity_id,
            favorites.c.phone_hash,
            favorites.c.created_at,
            favorites.c.utm
        ).where(favorites.c.phone_hash == phone_hash)
        
        # Фильтр по типу сущности
        if entity_type:
            if entity_type not in ["product", "location"]:
                raise HTTPException(status_code=400, detail="Тип сущности должен быть 'product' или 'location'")
            query = query.where(favorites.c.entity_type == entity_type)
        
        # Сортировка по дате добавления (новые сначала)
        query = query.order_by(desc(favorites.c.created_at))
        
        # Подсчет общего количества
        count_query = select(func.count()).where(favorites.c.phone_hash == phone_hash)
        if entity_type:
            count_query = count_query.where(favorites.c.entity_type == entity_type)
        total_count = await database.fetch_val(count_query)
        
        # Пагинация
        query = query.offset(offset).limit(size)
        
        # Выполнение запроса
        favorites_data = await database.fetch_all(query)
        
        # Формирование результата
        result = []
        for favorite in favorites_data:
            # Обрабатываем UTM данные
            utm_data = None
            if favorite.utm:
                try:
                    if isinstance(favorite.utm, str):
                        utm_data = json.loads(favorite.utm)
                    else:
                        utm_data = favorite.utm
                except:
                    utm_data = None
            
            result.append(schemas.FavoriteResponse(
                id=favorite.id,
                entity_type=favorite.entity_type,
                entity_id=favorite.entity_id,
                phone_hash=favorite.phone_hash,
                created_at=favorite.created_at,
                utm=utm_data
            ))
        
        return schemas.FavoriteListResponse(
            result=result,
            count=total_count,
            page=page,
            size=size
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Ошибка при получении избранного: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("/events/view")
async def get_view_events_info():
    """Информация о эндпоинте событий просмотра"""
    return {
        "message": "Используйте POST запрос для создания событий просмотра",
        "endpoint": "POST /mp/events/view",
        "example": {
            "entity_type": "product",
            "entity_id": 5,
            "listing_pos": 1,
            "listing_page": 1,
            "utm": {"source": "mobile_app", "campaign": "product_view"},
            "phone": "+79001111111"
        }
    }


@router.post("/events/view", response_model=schemas.ViewEventResponse)
async def create_view_event(request: schemas.ViewEventRequest):
    """Создание события просмотра товара или локации"""
    try:
        # Хэшируем телефон если он передан
        phone_hash = None
        if request.phone:
            import hashlib
            phone_hash = hashlib.sha256(request.phone.encode()).hexdigest()
        
        # Сохраняем событие в базу данных
        query = view_events.insert().values(
            entity_type=request.entity_type,
            entity_id=request.entity_id,
            listing_pos=request.listing_pos,
            listing_page=request.listing_page,
            phone_hash=phone_hash,
            utm=request.utm
        )
        
        await database.execute(query)
        
        return schemas.ViewEventResponse(
            success=True,
            message="Событие просмотра успешно сохранено"
        )
        
    except Exception as e:
        print(f"Ошибка при создании события просмотра: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

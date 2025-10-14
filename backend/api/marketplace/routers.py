from typing import Optional
from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import select, func, and_, or_, desc, asc, text

import api.marketplace.schemas as schemas
from api.marketplace import service
from api.marketplace.models import database

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
    
    return await service.get_products(
        phone=phone,
        lat=lat,
        lon=lon,
        city=city,
        page=page,
        size=size,
        sort=sort,
        category_filter=category,
        manufacturer_filter=manufacturer,
        min_price=min_price,
        max_price=max_price,
        tags_filter=tags,
        in_stock=in_stock,
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
    return await service.get_locations(city=city, lat=lat, lon=lon, radius=radius, page=page, size=size, sort=sort)


@router.post("/orders", response_model=schemas.MarketplaceOrderResponse)
async def create_marketplace_order(order_request: schemas.MarketplaceOrderRequest):
    """
    Создать заказ маркетплейса с автоматическим распределением по кешбоксам
    """
    return await service.create_order(order_request)


@router.get("/qr/{qr_hash}", response_model=schemas.QRResolveResponse)
async def resolve_qr_code(qr_hash: str):
    """
    Получить товар или локацию по QR-коду (MD5 хэш)
    """
    return await service.resolve_qr(qr_hash)


@router.post("/locations/{location_id}/reviews", response_model=schemas.ReviewResponse)
async def create_review(location_id: int, review_request: schemas.ReviewRequest):
    """
    Создать отзыв о локации
    """
    return await service.create_review(location_id, review_request)


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
    return await service.get_reviews(location_id=location_id, page=page, size=size, sort=sort)


@router.post("/favorites", response_model=schemas.FavoriteResponse)
async def add_to_favorites(favorite_request: schemas.FavoriteRequest):
    """
    Добавить товар или локацию в избранное
    """
    return await service.add_to_favorites(favorite_request)


@router.delete("/favorites/{favorite_id}")
async def remove_from_favorites(favorite_id: int, phone: str = Query(..., description="Номер телефона")):
    """
    Удалить элемент из избранного
    """
    return await service.remove_from_favorites(favorite_id, phone)


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
    return await service.get_favorites(phone=phone, page=page, size=size, entity_type=entity_type)


@router.get("/events/view")
async def get_view_events_info():from typing import Optional
from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import select, func, and_, or_, desc, asc, text

import api.marketplace.schemas as schemas
from api.marketplace import service
from api.marketplace.models import database

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
    
    return await service.get_products(
        phone=phone,
        lat=lat,
        lon=lon,
        city=city,
        page=page,
        size=size,
        sort=sort,
        category_filter=category,
        manufacturer_filter=manufacturer,
        min_price=min_price,
        max_price=max_price,
        tags_filter=tags,
        in_stock=in_stock,
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
    return await service.get_locations(city=city, lat=lat, lon=lon, radius=radius, page=page, size=size, sort=sort)


@router.post("/orders", response_model=schemas.MarketplaceOrderResponse)
async def create_marketplace_order(order_request: schemas.MarketplaceOrderRequest):
    """
    Создать заказ маркетплейса с автоматическим распределением по кешбоксам
    """
    return await service.create_order(order_request)


@router.get("/qr/{qr_hash}", response_model=schemas.QRResolveResponse)
async def resolve_qr_code(qr_hash: str):
    """
    Получить товар или локацию по QR-коду (MD5 хэш)
    """
    return await service.resolve_qr(qr_hash)


@router.post("/locations/{location_id}/reviews", response_model=schemas.ReviewResponse)
async def create_review(location_id: int, review_request: schemas.ReviewRequest):
    """
    Создать отзыв о локации
    """
    return await service.create_review(location_id, review_request)


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
    return await service.get_reviews(location_id=location_id, page=page, size=size, sort=sort)


@router.post("/favorites", response_model=schemas.FavoriteResponse)
async def add_to_favorites(favorite_request: schemas.FavoriteRequest):
    """
    Добавить товар или локацию в избранное
    """
    return await service.add_to_favorites(favorite_request)


@router.delete("/favorites/{favorite_id}")
async def remove_from_favorites(favorite_id: int, phone: str = Query(..., description="Номер телефона")):
    """
    Удалить элемент из избранного
    """
    return await service.remove_from_favorites(favorite_id, phone)


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
    return await service.get_favorites(phone=phone, page=page, size=size, entity_type=entity_type)


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
    return await service.create_view_event(request)

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
    return await service.create_view_event(request)

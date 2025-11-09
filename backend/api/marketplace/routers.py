from typing import Optional

from fastapi import APIRouter, Query, Depends

from api.marketplace.service.favorites_service.schemas import FavoriteRequest, FavoriteResponse, FavoriteListResponse
from api.marketplace.service.orders_service.schemas import MarketplaceOrderResponse, MarketplaceOrderRequest
from api.marketplace.service.products_list_service.schemas import MarketplaceProductList
from api.marketplace.service.qr_service.schemas import QRResolveResponse
from api.marketplace.service.review_service.schemas import UpdateReviewRequest, MarketplaceReview, CreateReviewRequest, \
    ReviewListResponse, ReviewListRequest
from api.marketplace.service.service import MarketplaceService
from api.marketplace.service.view_event_service.schemas import GetViewEventsRequest, CreateViewEventResponse, \
    CreateViewEventRequest
from api.marketplace.utils import get_marketplace_service

router = APIRouter(prefix="/mp", tags=["marketplace"])


@router.get("/products", response_model=MarketplaceProductList)
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
    in_stock: Optional[bool] = Query(None, description="Только товары в наличии"),
    service: MarketplaceService = Depends(get_marketplace_service)
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


# @router.get("/locations", response_model=MarketplaceLocationList)
# async def get_marketplace_locations(
#     city: Optional[str] = Query(None, description="Город"),
#     lat: Optional[float] = Query(None, description="Широта"),
#     lon: Optional[float] = Query(None, description="Долгота"),
#     radius: Optional[float] = Query(None, description="Радиус поиска в км"),
#     page: int = Query(1, ge=1, description="Номер страницы"),
#     size: int = Query(20, ge=1, le=100, description="Размер страницы"),
#     sort: Optional[str] = Query("name", description="Сортировка: name, distance, rating"),
#     service: MarketplaceService = Depends(get_marketplace_service)
# ):
#     """
#     Получить список публичных локаций для чекина
#     """
#     return await service.get_locations(city=city, lat=lat, lon=lon, radius=radius, page=page, size=size, sort=sort)


@router.post("/orders", response_model=MarketplaceOrderResponse)
async def create_marketplace_order(order_request: MarketplaceOrderRequest, service: MarketplaceService = Depends(get_marketplace_service)): # TODO: add UTM
    """
    Создать заказ маркетплейса с автоматическим распределением по кешбоксам
    """
    return await service.create_order(order_request)


@router.get("/qr/{qr_hash}", response_model=QRResolveResponse)
async def resolve_qr_code(qr_hash: str, service: MarketplaceService = Depends(get_marketplace_service)):
    """
    Получить товар или локацию по QR-коду (MD5 хэш)
    """
    return await service.resolve_qr(qr_hash)


@router.post("/reviews", response_model=MarketplaceReview)
async def create_review(review_request: CreateReviewRequest, service: MarketplaceService = Depends(get_marketplace_service)):
    return await service.create_review(review_request)


@router.get("/reviews", response_model=ReviewListResponse)
async def get_reviews(
    request: ReviewListRequest = Depends(),
    service: MarketplaceService = Depends(get_marketplace_service)
):
    return await service.get_reviews(request)


@router.patch("/reviews/{review_id}", response_model=MarketplaceReview)
async def update_review(
    review_id: int,
    request: UpdateReviewRequest,
    service: MarketplaceService = Depends(get_marketplace_service)
):
    return await service.update_review(review_id, request)


@router.post("/favorites", response_model=FavoriteResponse)
async def add_to_favorites(favorite_request: FavoriteRequest, service: MarketplaceService = Depends(get_marketplace_service)):
    """
    Добавить товар или локацию в избранное
    """
    return await service.add_to_favorites(favorite_request)


@router.delete("/favorites/{favorite_id}")
async def remove_from_favorites(favorite_id: int, phone: str = Query(..., description="Номер телефона"), service: MarketplaceService = Depends(get_marketplace_service)):
    """
    Удалить элемент из избранного
    """
    return await service.remove_from_favorites(favorite_id, phone)


@router.get("/favorites", response_model=FavoriteListResponse)
async def get_favorites(
    phone: str = Query(..., description="Номер телефона"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    size: int = Query(20, ge=1, le=100, description="Размер страницы"),
    service: MarketplaceService = Depends(get_marketplace_service)
):
    """
    Получить список избранного пользователя
    """
    return await service.get_favorites(contragent_phone=phone, page=page, size=size)


@router.get("/events/view")
async def get_view_events_info(request: GetViewEventsRequest = Depends(), service: MarketplaceService = Depends(get_marketplace_service)):
    """Информация о эндпоинте событий просмотра"""
    return await service.get_view_events(request)


@router.post("/events/view", response_model=CreateViewEventResponse)
async def create_view_event(request: CreateViewEventRequest, service: MarketplaceService = Depends(get_marketplace_service)):
    """Создание события просмотра товара или локации"""
    return await service.create_view_event(request)

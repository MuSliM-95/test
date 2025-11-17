


from fastapi import APIRouter, Query, Depends

from api.marketplace.service.favorites_service.schemas import FavoriteRequest, FavoriteResponse, FavoriteListResponse, \
    CreateFavoritesUtm
from api.marketplace.service.locations_service.schemas import LocationsListResponse, LocationsListRequest
from api.marketplace.service.orders_service.schemas import MarketplaceOrderResponse, MarketplaceOrderRequest, \
    CreateOrderUtm
from api.marketplace.service.product_cart_service.schemas import MarketplaceRemoveFromCartRequest, \
    MarketplaceCartResponse, MarketplaceGetCartRequest, MarketplaceAddToCartRequest
from api.marketplace.service.products_list_service.schemas import MarketplaceProductList, MarketplaceProductsRequest
from api.marketplace.service.qr_service.schemas import QRResolveResponse
from api.marketplace.service.review_service.schemas import UpdateReviewRequest, MarketplaceReview, CreateReviewRequest, \
    ReviewListResponse, ReviewListRequest
from api.marketplace.service.service import MarketplaceService
from api.marketplace.service.view_event_service.schemas import GetViewEventsRequest, CreateViewEventResponse, \
    CreateViewEventRequest, ViewEventsUtm
from api.marketplace.utils import get_marketplace_service

router = APIRouter(prefix="/mp", tags=["marketplace"])


@router.get("/products", response_model=MarketplaceProductList)
async def get_marketplace_products(
    request: MarketplaceProductsRequest = Depends(),
    service: MarketplaceService = Depends(get_marketplace_service)
):
    """
    Получение всех публичных товаров маркетплейса
    
    Фильтрует только товары с:
    - price_type = 'chatting'
    """
    
    return await service.get_products(request)


@router.get("/locations", response_model=LocationsListResponse)
async def get_marketplace_locations(
    request: LocationsListRequest = Depends(),
    service: MarketplaceService = Depends(get_marketplace_service)
):
    """
    Получить список публичных локаций
    """
    return await service.get_locations(request)


@router.post("/orders", response_model=MarketplaceOrderResponse)
async def create_marketplace_order(order_request: MarketplaceOrderRequest, utm: CreateOrderUtm = Depends(), service: MarketplaceService = Depends(get_marketplace_service)):
    """
    Создать заказ маркетплейса с автоматическим распределением по кешбоксам
    """
    return await service.create_order(order_request, utm)


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
async def add_to_favorites(favorite_request: FavoriteRequest, utm: CreateFavoritesUtm = Depends(), service: MarketplaceService = Depends(get_marketplace_service)):
    """
    Добавить товар или локацию в избранное
    """
    return await service.add_to_favorites(favorite_request, utm)


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
    """Информация о событиях просмотра"""
    return await service.get_view_events(request)


@router.post("/events/view", response_model=CreateViewEventResponse)
async def create_view_event(request: CreateViewEventRequest, utm: ViewEventsUtm = Depends(), service: MarketplaceService = Depends(get_marketplace_service)):
    """Создание события просмотра товара"""
    return await service.create_view_event(request, utm)


@router.post("/cart/add", response_model=MarketplaceCartResponse)
async def add_to_cart(
        request: MarketplaceAddToCartRequest,
        service: MarketplaceService = Depends(get_marketplace_service)
):
    """
    Добавить товар в корзину покупок

    Если корзина не существует, она будет создана автоматически.
    Если товар уже есть в корзине, количество будет увеличено.
    """
    return await service.add_to_cart(request)


@router.get("/cart", response_model=MarketplaceCartResponse)
async def get_cart(
        request: MarketplaceGetCartRequest = Depends(),
        service: MarketplaceService = Depends(get_marketplace_service)
):
    """
    Получить содержимое корзины покупок для указанного номера телефона
    """
    return await service.get_cart(request)


@router.delete("/cart/remove", response_model=MarketplaceCartResponse)
async def remove_from_cart(
        request: MarketplaceRemoveFromCartRequest,
        service: MarketplaceService = Depends(get_marketplace_service)
):
    """
    Удалить товар из корзины покупок

    Если warehouse_id не указан, будет удален товар без привязки к складу.
    Если указан - будет удален товар конкретного склада.
    """

    return await service.remove_from_cart(request)



from fastapi import File, UploadFile

@router.get("/categories/", response_model=GlobalCategoryList)
async def get_global_categories(
    limit: int = 100,
    offset: int = 0,
    service: MarketplaceService = Depends(get_marketplace_service)
):
    return await service.get_global_categories(limit=limit, offset=offset)


@router.get("/categories/tree/", response_model=GlobalCategoryTreeList)
async def get_global_categories_tree(
    service: MarketplaceService = Depends(get_marketplace_service)
):
    return await service.get_global_categories_tree()


@router.get("/categories/{category_id}/", response_model=GlobalCategoryTree)
async def get_global_category(
    category_id: int,
    service: MarketplaceService = Depends(get_marketplace_service)
):
    return await service.get_global_category(category_id)


@router.post("/categories/", response_model=GlobalCategoryTree, status_code=201)
async def create_global_category(
    category: GlobalCategoryCreate,
    service: MarketplaceService = Depends(get_marketplace_service)
):
    return await service.create_global_category(category)


@router.patch("/categories/{category_id}/", response_model=GlobalCategoryTree)
async def update_global_category(
    category_id: int,
    category_update: GlobalCategoryUpdate,
    service: MarketplaceService = Depends(get_marketplace_service)
):
    return await service.update_global_category(category_id, category_update)


@router.delete("/categories/{category_id}/")
async def delete_global_category(
    category_id: int,
    service: MarketplaceService = Depends(get_marketplace_service)
):
    return await service.delete_global_category(category_id)


@router.post("/categories/{category_id}/upload_image/")
async def upload_category_image(
    category_id: int,
    file: UploadFile = File(...),
    service: MarketplaceService = Depends(get_marketplace_service)
):
    return await service.upload_category_image(category_id, file)

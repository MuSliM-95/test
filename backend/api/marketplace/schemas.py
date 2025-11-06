from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime

from api.docs_sales.schemas import DeliveryInfoSchema


class AvailableWarehouse(BaseModel):
    warehouse_id: int
    organization_id: int
    warehouse_name: str
    warehouse_address: str

    class Config:
        orm_mode = True


class MarketplaceProduct(BaseModel):
    """Модель товара для маркетплейса"""
    id: int
    name: str
    description_short: Optional[str] = None
    description_long: Optional[str] = None
    code: Optional[str] = None
    unit_name: Optional[str] = None
    cashbox_id: int
    category_name: Optional[str] = None
    manufacturer_name: Optional[str] = None
    price: float
    price_type: str
    geo_point: Optional[str] = None  # POINT as string
    city: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    images: Optional[List[str]] = None
    barcodes: Optional[List[str]] = None

    # Новые поля для расширенной функциональности
    listing_pos: Optional[int] = None  # Позиция в выдаче для аналитики
    is_ad_pos: Optional[bool] = False  # Рекламное размещение
    tags: Optional[List[str]] = None  # Теги товара
    variations: Optional[List[dict]] = None  # Вариации товара
    stock_quantity: Optional[float] = None  # Остатки

    seller_name: Optional[str] = None  # Имя селлера
    seller_photo: Optional[str] = None  # Фото селлера

    rating: Optional[float] = None  # Рейтинг 1-5
    reviews_count: Optional[int] = None  # Количество отзывов
    distance: Optional[float] = None  # Расстояние до клиента (если передана геолокация)

    available_warehouses: Optional[List[AvailableWarehouse]] = None

    class Config:
        orm_mode = True


class MarketplaceProductList(BaseModel):
    """Список товаров маркетплейса"""
    result: List[MarketplaceProduct]
    count: int
    page: int
    size: int


class MarketplaceLocation(BaseModel):
    """Модель локации для маркетплейса"""
    id: int
    name: str
    address: Optional[str] = None
    cashbox_id: Optional[int] = None
    admin_id: Optional[int] = None
    avg_rating: Optional[float] = None
    reviews_count: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        orm_mode = True


class MarketplaceLocationList(BaseModel):
    """Список локаций маркетплейса"""
    result: List[MarketplaceLocation]
    count: int
    page: int
    size: int


class CustomerInfo(BaseModel):
    """Информация о заказчике"""
    phone: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    name: Optional[str] = None


class RecipientInfo(BaseModel):
    """Информация о получателе заказа"""
    phone: Optional[str] = None  # Если отличается от заказчика
    name: Optional[str] = None
    lat: Optional[float] = None  # Место получения
    lon: Optional[float] = None


class MarketplaceOrderGood(BaseModel):
    cashbox_id: int
    nomenclature_id: int
    organization_id: int
    warehouse_id: int # ID помещения
    quantity: int = 1  # Количество товара


class MarketplaceOrderRequest(BaseModel):
    """Запрос на создание заказа маркетплейса"""
    goods: List[MarketplaceOrderGood]
    utm: Optional[dict] = None  # UTM метки
    delivery: DeliveryInfoSchema
    contragent_id: int
    recipient: Optional[RecipientInfo] = None  # Информация о получателе (если отличается от заказчика)
    order_type: str = "self"  # Тип заказа: self, other, corporate, gift, proxy


class MarketplaceOrderResponse(BaseModel):
    """Ответ на создание заказа маркетплейса"""
    # order_id: str
    # status: str
    message: str
    # estimated_delivery: Optional[str] = None
    # cashbox_assignments: Optional[List[dict]] = None


class QRResolveResponse(BaseModel):
    """Ответ QR-резолвера"""
    type: str  # "product" или "location"
    entity: dict  # Данные товара или локации
    qr_hash: str
    resolved_at: datetime


class ReviewRequest(BaseModel):
    """Запрос на создание отзыва"""
    rating: int  # 1-5
    text: str
    phone: str
    utm: Optional[dict] = None


class ReviewResponse(BaseModel):
    """Ответ с отзывом"""
    id: int
    location_id: int
    rating: int
    text: str
    phone_hash: str
    status: str  # pending, visible, hidden
    created_at: datetime
    utm: Optional[dict] = None

    class Config:
        orm_mode = True


class ReviewListResponse(BaseModel):
    """Список отзывов"""
    result: List[ReviewResponse]
    count: int
    page: int
    size: int
    avg_rating: Optional[float] = None


class FavoriteRequest(BaseModel):
    """Запрос на добавление в избранное"""
    entity_type: str  # Только "product"
    entity_id: int
    phone: str
    utm: Optional[dict] = None


class FavoriteResponse(BaseModel):
    """Ответ с избранным"""
    id: int
    entity_type: str
    entity_id: int
    phone_hash: str
    created_at: datetime
    utm: Optional[dict] = None


class FavoriteListResponse(BaseModel):
    """Список избранного"""
    result: List[FavoriteResponse]
    count: int
    page: int
    size: int


class ViewEventRequest(BaseModel):
    """Запрос на создание события просмотра"""
    entity_type: str  # "product" или "location"
    entity_id: int
    listing_pos: Optional[int] = None  # Позиция в выдаче
    listing_page: Optional[int] = None  # Страница выдачи
    utm: Optional[Dict[str, Any]] = None
    phone: Optional[str] = None  # Для аналитики


class ViewEventResponse(BaseModel):
    """Ответ на создание события просмотра"""
    success: bool
    message: str


class MarketplaceProductFilters(BaseModel):
    """Фильтры для товаров маркетплейса"""
    phone: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    city: Optional[str] = None
    page: int = 1
    size: int = 20
    sort: Optional[str] = None  # 'distance', 'price', 'name', 'created_at'

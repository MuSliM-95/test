from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel


class AvailableWarehouse(BaseModel):
    warehouse_id: int
    organization_id: int
    warehouse_name: str
    warehouse_address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    distance_to_client: Optional[float] = None

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
    listing_page: Optional[int] = None
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

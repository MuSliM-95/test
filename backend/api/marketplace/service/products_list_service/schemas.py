from datetime import datetime
from enum import Enum
from typing import Optional, List, Literal

from pydantic import BaseModel, Field


class AvailableWarehouse(BaseModel):
    warehouse_id: int
    organization_id: int
    warehouse_name: str
    warehouse_address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    distance_to_client: Optional[float] = None
    current_amount: Optional[float] = None

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
    created_at: datetime
    updated_at: datetime
    images: Optional[List[str]] = None
    barcodes: Optional[List[str]] = None
    type: Optional[str] = None

    distance: Optional[float] = None

    # Новые поля для расширенной функциональности
    listing_pos: Optional[int] = None  # Позиция в выдаче для аналитики
    listing_page: Optional[int] = None
    is_ad_pos: Optional[bool] = False  # Рекламное размещение

    tags: Optional[List[str]] = None  # Теги товара
    variations: Optional[List[dict]] = None  # Вариации товара
    current_amount: Optional[float] = None  # Остатки

    seller_name: Optional[str] = None  # Имя селлера
    seller_photo: Optional[str] = None  # Фото селлера
    seller_description: Optional[str] = None # Описание селлера

    total_sold: Optional[int] = None

    rating: Optional[float] = None  # Рейтинг 1-5
    reviews_count: Optional[int] = None  # Количество отзывов

    available_warehouses: Optional[List[AvailableWarehouse]] = None

    class Config:
        orm_mode = True

class MarketplaceProductAttribute(BaseModel):
    """Атрибуты товара"""
    name: str
    value: str

class MarketplaceProductDetail(MarketplaceProduct):
    """Дополненная модель товара для маркетплейса"""
    seo_title: Optional[str] = None
    seo_description: Optional[str] = None
    seo_keywords: Optional[List[str]] = None
    attributes: Optional[List[MarketplaceProductAttribute]] = None
    nomenclatures: Optional[List[dict]] = None
    processing_time_ms: Optional[int] = None

    class Config:
        orm_mode = True

class MarketplaceProductList(BaseModel):
    """Список товаров маркетплейса"""
    result: List[MarketplaceProduct]
    count: int
    page: int
    size: int
    processing_time_ms: Optional[int] = None


class MarketplaceSort(Enum):
    distance = "distance"
    price = "price"
    name = "name"
    rating = "rating"
    total_sold = "total_sold"
    created_at = "created_at"
    updated_at = "updated_at"

class MarketplaceProductsRequest(BaseModel):
    phone: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    page: int = 1
    size: int = Field(default=20, le=100)
    sort_by: Optional[MarketplaceSort] = None
    sort_order: Optional[Literal["asc", "desc"]] = "desc"
    category: Optional[str] = None
    manufacturer: Optional[str] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    # tags: Optional[str] = None
    in_stock: Optional[bool] = None

    rating_from: Optional[int] = None
    rating_to: Optional[int] = None

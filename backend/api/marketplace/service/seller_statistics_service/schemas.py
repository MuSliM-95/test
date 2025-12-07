from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class SellerStatisticsItem(BaseModel):
    id: int
    seller_name: Optional[str] = None
    seller_description: Optional[str] = None
    seller_photo: Optional[str] = None

    rating: Optional[float] = None
    reviews_count: Optional[int] = None

    orders_total: Optional[int] = None
    orders_completed: Optional[int] = None

    registration_date: Optional[int] = None
    last_order_date: Optional[datetime] = None

    active_warehouses: int
    total_products: int


class SellerStatisticsResponse(BaseModel):
    sellers: List[SellerStatisticsItem]
from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel


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

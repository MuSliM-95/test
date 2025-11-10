from datetime import datetime
from typing import List

from pydantic import BaseModel

from api.marketplace.schemas import BaseMarketplaceUtm, UtmEntityType


class FavoriteRequest(BaseModel):
    """Запрос на добавление в избранное"""
    nomenclature_id: int
    contragent_phone: str


class FavoriteResponse(BaseModel):
    """Ответ с избранным"""
    id: int
    nomenclature_id: int
    contagent_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class FavoriteListResponse(BaseModel):
    """Список избранного"""
    result: List[FavoriteResponse]
    count: int
    page: int
    size: int

class CreateFavoritesUtm(BaseMarketplaceUtm):
    entity_type: UtmEntityType = UtmEntityType.favorites

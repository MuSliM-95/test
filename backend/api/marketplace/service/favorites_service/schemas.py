from datetime import datetime
from typing import List, Optional

from api.marketplace.schemas import BaseMarketplaceUtm, UtmEntityType
from pydantic import BaseModel


class FavoriteRequest(BaseModel):
    """Запрос на добавление в избранное"""

    nomenclature_id: int
    contragent_phone: str
    # UTM параметры (опциональные, могут быть в body или query string)
    utm_term: Optional[str] = None
    ref_user: Optional[str] = None


class FavoriteResponse(BaseModel):
    """Ответ с избранным"""

    id: int
    nomenclature_id: int
    phone: str
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

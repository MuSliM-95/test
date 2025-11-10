from typing import Optional, List

from pydantic import BaseModel

from api.docs_sales.schemas import DeliveryInfoSchema
from api.marketplace.schemas import BaseMarketplaceUtm, UtmEntityType


class MarketplaceOrderGood(BaseModel):
    nomenclature_id: int
    organization_id: Optional[int] = None
    warehouse_id: Optional[int] = None # ID помещения
    quantity: int = 1  # Количество товара


class MarketplaceOrderRequest(BaseModel):
    """Запрос на создание заказа маркетплейса"""
    goods: List[MarketplaceOrderGood]
    delivery: DeliveryInfoSchema
    contragent_id: int
    # order_type: str = "self"  # Тип заказа: self, other, corporate, gift, proxy
    client_lat: Optional[float] = None
    client_lon: Optional[float] = None


class MarketplaceOrderResponse(BaseModel):
    """Ответ на создание заказа маркетплейса"""
    # order_id: str
    # status: str
    message: str
    # estimated_delivery: Optional[str] = None
    # cashbox_assignments: Optional[List[dict]] = None

class CreateOrderUtm(BaseMarketplaceUtm):
    entity_type: UtmEntityType = UtmEntityType.docs_sales

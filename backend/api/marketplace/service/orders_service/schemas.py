from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field

from api.docs_sales.schemas import DeliveryInfoSchema
from api.marketplace.schemas import BaseMarketplaceUtm, UtmEntityType


class MarketplaceOrderGood(BaseModel):
    nomenclature_id: int
    warehouse_id: Optional[int] = None # ID помещения
    quantity: int = 1  # Количество товара
    is_from_cart: Optional[bool] = False


class MarketplaceOrderRequest(BaseModel):
    """Запрос на создание заказа маркетплейса"""
    goods: List[MarketplaceOrderGood]
    delivery: DeliveryInfoSchema
    contragent_phone: str
    # order_type: str = "self"  # Тип заказа: self, other, corporate, gift, proxy
    client_lat: Optional[float] = None
    client_lon: Optional[float] = None
    additional_data: List[Dict[str, Any]] = Field(default_factory=list)

class MarketplaceOrderResponse(BaseModel):
    """Ответ на создание заказа маркетплейса"""
    # order_id: str
    # status: str
    message: str
    processing_time_ms: Optional[int] = None
    # estimated_delivery: Optional[str] = None
    # cashbox_assignments: Optional[List[dict]] = None

class CreateOrderUtm(BaseMarketplaceUtm):
    entity_type: UtmEntityType = UtmEntityType.docs_sales

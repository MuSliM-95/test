from typing import Optional, List

from pydantic import BaseModel

from api.docs_sales.schemas import DeliveryInfoSchema


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

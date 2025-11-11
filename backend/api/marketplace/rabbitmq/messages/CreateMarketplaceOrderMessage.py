from typing import List, Optional

from api.docs_sales.schemas import DeliveryInfoSchema
from api.marketplace.service.orders_service.schemas import MarketplaceOrderGood, CreateOrderUtm
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class OrderGoodMessage(MarketplaceOrderGood):
    organization_id: int

class CreateMarketplaceOrderMessage(BaseModelMessage):
    cashbox_id: int
    contragent_id: int
    goods: List[OrderGoodMessage]
    delivery_info: DeliveryInfoSchema
    utm: Optional[CreateOrderUtm] = None

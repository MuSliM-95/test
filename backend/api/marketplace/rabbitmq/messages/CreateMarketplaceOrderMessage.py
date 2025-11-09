from typing import List

from api.docs_sales.schemas import DeliveryInfoSchema
from api.marketplace.service.orders_service.schemas import MarketplaceOrderGood
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class CreateMarketplaceOrderMessage(BaseModelMessage):
    cashbox_id: int
    contragent_id: int
    goods: List[MarketplaceOrderGood]
    delivery_info: DeliveryInfoSchema

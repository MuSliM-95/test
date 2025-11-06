from typing import List

from pydantic import validator

from api.docs_sales.schemas import DeliveryInfoSchema
from api.marketplace.schemas import MarketplaceOrderGood
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class CreateMarketplaceOrderMessage(BaseModelMessage):
    cashbox_id: int
    contragent_id: int
    goods: List[MarketplaceOrderGood]
    delivery_info: DeliveryInfoSchema

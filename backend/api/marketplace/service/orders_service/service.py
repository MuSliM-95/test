import uuid

from sqlalchemy import select

from api.marketplace.rabbitmq.messages.CreateMarketplaceOrderMessage import CreateMarketplaceOrderMessage
from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.orders_service.schemas import MarketplaceOrderResponse, MarketplaceOrderGood, \
    MarketplaceOrderRequest
from database.db import nomenclature, database


class MarketplaceOrdersService(BaseMarketplaceService):
    async def create_order(self, order_request: MarketplaceOrderRequest) -> MarketplaceOrderResponse:
        # группируем товары по cashbox
        # TODO: add autosetuping warehouse_id and org_id
        goods_dict: dict[int, list[MarketplaceOrderGood]] = {}
        for good in order_request.goods:
            cashbox_query = select(nomenclature.c.cashbox).where(nomenclature.c.id == good.nomenclature_id)
            cashbox_id = (await database.fetch_one(cashbox_query)).id

            if goods_dict.get(cashbox_id):
                goods_dict[cashbox_id].append(good)
            else:
                goods_dict[cashbox_id] = [good]

        for cashbox, goods in goods_dict.items():
            await self.__rabbitmq.publish(
                CreateMarketplaceOrderMessage(
                    message_id=uuid.uuid4(),
                    cashbox_id=cashbox,
                    contragent_id=order_request.contragent_id,
                    goods=goods,
                    delivery_info=order_request.delivery,
                ),
                routing_key='create_marketplace_order',
            )

        return MarketplaceOrderResponse(
            message="Заказ создан и отправлен на обработку"
        )

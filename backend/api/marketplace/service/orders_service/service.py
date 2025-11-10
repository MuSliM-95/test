import uuid
from abc import ABC, abstractmethod
from typing import List, Optional

from fastapi import HTTPException
from sqlalchemy import select

from api.marketplace.rabbitmq.messages.CreateMarketplaceOrderMessage import CreateMarketplaceOrderMessage
from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.orders_service.schemas import MarketplaceOrderResponse, MarketplaceOrderGood, \
    MarketplaceOrderRequest, CreateOrderUtm
from api.marketplace.service.products_list_service.schemas import AvailableWarehouse
from database.db import nomenclature, database


class MarketplaceOrdersService(BaseMarketplaceService, ABC):
    @abstractmethod
    async def _fetch_available_warehouses(
            self,
            nomenclature_id: int,
            client_lat: Optional[float] = None,
            client_lon: Optional[float] = None
    ) -> List[AvailableWarehouse]:
        ...

    async def create_order(self, order_request: MarketplaceOrderRequest, utm: CreateOrderUtm) -> MarketplaceOrderResponse:
        # группируем товары по cashbox
        goods_dict: dict[int, list[MarketplaceOrderGood]] = {}
        for good in order_request.goods:
            cashbox_query = select(nomenclature.c.cashbox).where(nomenclature.c.id == good.nomenclature_id)
            cashbox_id = (await database.fetch_one(cashbox_query)).cashbox

            if goods_dict.get(cashbox_id):
                goods_dict[cashbox_id].append(good)
            else:
                goods_dict[cashbox_id] = [good]

            if good.warehouse_id is None:
                if all([order_request.client_lat, order_request.client_lon]):
                    warehouse = (await self._fetch_available_warehouses(
                        nomenclature_id=good.nomenclature_id,
                        client_lat=order_request.client_lat,
                        client_lon=order_request.client_lon
                    ))[0]
                    good.warehouse_id = warehouse.warehouse_id
                    good.organization_id = warehouse.organization_id
                else:
                    raise HTTPException(status_code=422, detail='Нужно указать либо склад, либо координаты клиента')

        for cashbox, goods in goods_dict.items():
            await self._rabbitmq.publish(
                CreateMarketplaceOrderMessage(
                    message_id=uuid.uuid4(),
                    cashbox_id=cashbox,
                    contragent_id=order_request.contragent_id,
                    goods=goods,
                    delivery_info=order_request.delivery,
                    utm=utm,
                ),
                routing_key='create_marketplace_order',
            )

        return MarketplaceOrderResponse(
            message="Заказ создан и отправлен на обработку"
        )

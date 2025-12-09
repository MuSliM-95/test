import uuid
from abc import ABC, abstractmethod
from typing import List, Optional

from fastapi import HTTPException
from sqlalchemy import select

from api.marketplace.rabbitmq.messages.CreateMarketplaceOrderMessage import CreateMarketplaceOrderMessage, \
    OrderGoodMessage
from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.orders_service.schemas import MarketplaceOrderResponse, MarketplaceOrderGood, \
    MarketplaceOrderRequest, CreateOrderUtm
from api.marketplace.service.products_list_service.schemas import AvailableWarehouse
from database.db import nomenclature, database, warehouse_balances, warehouses


class MarketplaceOrdersService(BaseMarketplaceService, ABC):
    async def _fetch_available_warehousess(
            self,
            nomenclature_id: int,
            client_lat: Optional[float] = None,
            client_lon: Optional[float] = None
    ) -> List[AvailableWarehouse]:
        """
        Заглушка: возвращает первый попавшийся склад с положительным остатком для указанной номенклатуры
        """
        query = select(
            warehouse_balances.c.warehouse_id,
            warehouse_balances.c.organization_id,
            warehouse_balances.c.current_amount,
            warehouses.c.name.label("warehouse_name"),
        ).select_from(
            warehouse_balances.join(
                warehouses,
                warehouses.c.id == warehouse_balances.c.warehouse_id,
            )
        ).where(
            warehouse_balances.c.nomenclature_id == nomenclature_id,
            warehouse_balances.c.current_amount > 0,
        ).limit(1)

        result = await database.fetch_one(query)

        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Нет доступных складов с товаром nomenclature_id={nomenclature_id}"
            )

        return [
            AvailableWarehouse(
                warehouse_id=result.warehouse_id,
                organization_id=result.organization_id,
                warehouse_name=result.warehouse_name,
                current_amount=result.current_amount,
            )
        ]

    @staticmethod
    async def __transform_good(good: OrderGoodMessage) -> OrderGoodMessage:
        if good.organization_id == -1:
            org_id_query = select(warehouse_balances.c.organization_id).where(
                warehouse_balances.c.warehouse_id == good.warehouse_id,
                warehouse_balances.c.nomenclature_id == good.nomenclature_id,
            )
            org_id = (await database.fetch_one(org_id_query)).organization_id
            good.organization_id = org_id

        return good


    async def create_order(self, order_request: MarketplaceOrderRequest, utm: CreateOrderUtm) -> MarketplaceOrderResponse:
        # группируем товары по cashbox
        goods_dict: dict[int, list[OrderGoodMessage]] = {}
        for good in order_request.goods:
            cashbox_query = select(nomenclature.c.cashbox).where(nomenclature.c.id == good.nomenclature_id)
            cashbox_id = (await database.fetch_one(cashbox_query)).cashbox

            good = OrderGoodMessage(
                organization_id=-1, # дефолтное значение, которое нужно изменить
                **good.dict()
            )

            if good.warehouse_id is None:
                if all([order_request.client_lat, order_request.client_lon]):
                    warehouses = await self._fetch_available_warehousess(
                        nomenclature_id=good.nomenclature_id,
                        client_lat=order_request.client_lat,
                        client_lon=order_request.client_lon
                    )

                    if not warehouses:
                        raise HTTPException(
                            status_code=404,
                            detail=f'Нет доступных складов для товара nomenclature_id={good.nomenclature_id}'
                        )

                    warehouse = warehouses[0]
                    good.warehouse_id = warehouse.warehouse_id
                    good.organization_id = warehouse.organization_id
                else:
                    raise HTTPException(status_code=422, detail='Нужно указать либо склад, либо координаты клиента')
            else:
                good = await self.__transform_good(good)

            if goods_dict.get(cashbox_id):
                goods_dict[cashbox_id].append(good)
            else:
                goods_dict[cashbox_id] = [good]

        contragent_id = await self._get_contragent_id_by_phone(order_request.contragent_phone)
        for cashbox, goods in goods_dict.items():
            await self._rabbitmq.publish(
                CreateMarketplaceOrderMessage(
                    message_id=uuid.uuid4(),
                    cashbox_id=cashbox,
                    contragent_id=contragent_id,
                    goods=goods,
                    delivery_info=order_request.delivery,
                    utm=utm,
                    additional_data=order_request.additional_data,
                ),
                routing_key='create_marketplace_order',
            )

        return MarketplaceOrderResponse(
            message="Заказ создан и отправлен на обработку"
        )

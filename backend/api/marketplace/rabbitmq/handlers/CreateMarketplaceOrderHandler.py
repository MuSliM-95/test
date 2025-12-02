import datetime
import json
from typing import Mapping, Any, Optional

from aio_pika import IncomingMessage
from fastapi import HTTPException
from sqlalchemy import select

from api.docs_sales.api.routers import delivery_info as create_delivery_info
from api.docs_sales.schemas import Create as CreateDocsSales
from api.docs_sales.schemas import CreateMass as CreateMassDocsSales
from api.docs_sales.schemas import Item as DocsSalesItem
from api.docs_sales.web.views.CreateDocsSalesView import CreateDocsSalesView
from api.docs_sales_utm_tags.schemas import CreateUTMTag
from api.docs_sales_utm_tags.service import get_docs_sales_utm_service
from api.marketplace.rabbitmq.messages.CreateMarketplaceOrderMessage import CreateMarketplaceOrderMessage
from api.marketplace.rabbitmq.utils import get_rabbitmq_factory
from api.marketplace.service.orders_service.schemas import MarketplaceOrderGood, CreateOrderUtm
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from database.db import users_cboxes_relation, database, prices


class CreateMarketplaceOrderHandler(IEventHandler[CreateMarketplaceOrderMessage]):
    @staticmethod
    async def __add_utm(token, entity_id: int, utm: CreateOrderUtm):
        service = await get_docs_sales_utm_service()
        try:
            await service.create_utm_tag(token, entity_id, CreateUTMTag(**utm.dict()))
        except HTTPException:
            pass

    async def __call__(self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None):
        data = CreateMarketplaceOrderMessage(**event)
        token_query = select(users_cboxes_relation.c.token).where(users_cboxes_relation.c.cashbox_id == data.cashbox_id)
        token = (await database.fetch_one(token_query)).token
        comment = json.dumps(data.additional_data, ensure_ascii=False) if data.additional_data else ""

        # разделить по warehouses
        warehouses_dict: dict[tuple[int, int], list[MarketplaceOrderGood]] = {}

        for good in data.goods:
            if warehouses_dict.get((good.warehouse_id, good.organization_id)):
                warehouses_dict[(good.warehouse_id, good.organization_id)].append(good)
            else:
                warehouses_dict[(good.warehouse_id, good.organization_id)] = [good]

        for warehouse_and_organization, goods in warehouses_dict.items():
            # отправить запросы в create
            create_docs_sales_view = CreateDocsSalesView(
                rabbitmq_messaging_factory=get_rabbitmq_factory()
            )
            create_result = await create_docs_sales_view.__call__(
                token=token,
                docs_sales_data=CreateMassDocsSales(
                    __root__=[
                        CreateDocsSales(
                            contragent=data.contragent_id,
                            organization=warehouse_and_organization[1],
                            warehouse=warehouse_and_organization[0],
                            goods=[
                                DocsSalesItem(
                                    price=(await database.fetch_one(select(prices.c.price).where(prices.c.nomenclature == good.nomenclature_id))).price,
                                    quantity=good.quantity,
                                    nomenclature=good.nomenclature_id
                                ) for good in goods
                            ],
                            dated=datetime.datetime.now().timestamp(),
                            status=True,
                            is_marketplace_order=True,
                            comment=comment,
                            # loyality_card_id=,
                        )
                    ]
                )
            )
            docs_sales_id = create_result[0]['id']
            # выставляем delivery_info
            await create_delivery_info(
                token=token,
                idx=docs_sales_id,
                data=data.delivery_info
            )
            # добавляем utm
            if data.utm:
                await self.__add_utm(token, docs_sales_id, data.utm)

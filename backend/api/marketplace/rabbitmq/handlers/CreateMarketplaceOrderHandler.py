import datetime
from typing import Mapping, Any, Optional

from aio_pika import IncomingMessage
from sqlalchemy import select

from api.docs_sales.schemas import Create as CreateDocsSales
# from api.docs_sales.api.routers import create as create_docs_sales
from api.docs_sales.schemas import CreateMass as CreateMassDocsSales
from api.docs_sales.schemas import Item as DocsSalesItem
from api.docs_sales.web.views.CreateDocsSalesView import CreateDocsSalesView
from api.marketplace.rabbitmq.messages.CreateMarketplaceOrderMessage import CreateMarketplaceOrderMessage
from api.marketplace.rabbitmq.utils import get_rabbitmq_factory
from api.marketplace.service.orders_service.schemas import MarketplaceOrderGood
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from database.db import users_cboxes_relation, database, prices


class CreateMarketplaceOrderHandler(IEventHandler[CreateMarketplaceOrderMessage]):
    async def __call__(self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None):
        data = CreateMarketplaceOrderMessage(**event)
        token_query = select(users_cboxes_relation.c.token).where(users_cboxes_relation.c.cashbox_id == data.cashbox_id)
        token = (await database.fetch_one(token_query)).token

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
            await create_docs_sales_view.__call__(
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
                            # loyality_card_id=,
                        )
                    ]
                )
            )
            # TODO: выставляем delivery_info
            # await create_delivery_info(
            #     token=token,
            #     idx=
            # )
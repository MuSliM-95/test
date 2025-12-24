import datetime
import json
import traceback
from typing import Any, Mapping, Optional

from aio_pika import IncomingMessage
from api.docs_sales.api.routers import delivery_info as create_delivery_info
from api.docs_sales.schemas import (
    Create as CreateDocsSales,
    CreateMass as CreateMassDocsSales,
    Item as DocsSalesItem,
)
from api.docs_sales.web.views.CreateDocsSalesView import CreateDocsSalesView
from api.docs_sales_utm_tags.schemas import CreateUTMTag
from api.docs_sales_utm_tags.service import get_docs_sales_utm_service
from api.marketplace.rabbitmq.messages.CreateMarketplaceOrderMessage import (
    CreateMarketplaceOrderMessage,
)
from api.marketplace.rabbitmq.utils import get_rabbitmq_factory
from api.marketplace.service.orders_service.schemas import (
    CreateOrderUtm,
    MarketplaceOrderGood,
)
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from database.db import database, marketplace_orders, prices, users_cboxes_relation
from fastapi import HTTPException
from sqlalchemy import select, update


class CreateMarketplaceOrderHandler(IEventHandler[CreateMarketplaceOrderMessage]):
    @staticmethod
    async def __add_utm(token, entity_id: int, utm: CreateOrderUtm):
        service = await get_docs_sales_utm_service()
        try:
            await service.create_utm_tag(token, entity_id, CreateUTMTag(**utm.dict()))
        except HTTPException:
            # UTM не должен ломать создание заказа
            pass

    @staticmethod
    async def __set_marketplace_order_status(
        marketplace_order_id: int,
        status: str,
        error: Optional[str] = None,
    ) -> None:
        values = {"status": status}
        if error is not None:
            # Обрежем, чтобы не уронить UPDATE на слишком длинном тексте
            values["error"] = error[:8000]

        stmt = update(marketplace_orders).where(
            marketplace_orders.c.id == marketplace_order_id
        )

        # ВАЖНО: если уже стоит error — не затираем его на success/processing
        if status != "error":
            stmt = stmt.where(marketplace_orders.c.status != "error")

        await database.execute(stmt.values(**values))

    async def __call__(
        self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None
    ):
        data = CreateMarketplaceOrderMessage(**event)

        # Статус: начали обработку
        await self.__set_marketplace_order_status(
            data.marketplace_order_id, "processing"
        )

        try:
            token_query = select(users_cboxes_relation.c.token).where(
                users_cboxes_relation.c.cashbox_id == data.cashbox_id
            )
            token_row = await database.fetch_one(token_query)
            if not token_row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Не найден token для cashbox_id={data.cashbox_id}",
                )
            token = token_row.token

            comment = (
                json.dumps(data.additional_data, ensure_ascii=False)
                if data.additional_data
                else ""
            )

            # разделить по warehouses
            warehouses_dict: dict[tuple[int, int], list[MarketplaceOrderGood]] = {}

            for good in data.goods:
                key = (good.warehouse_id, good.organization_id)
                if warehouses_dict.get(key):
                    warehouses_dict[key].append(good)
                else:
                    warehouses_dict[key] = [good]

            docs_sales_ids: list[int] = []

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
                                        price=(
                                            await database.fetch_one(
                                                select(prices.c.price).where(
                                                    prices.c.nomenclature
                                                    == good.nomenclature_id
                                                )
                                            )
                                        ).price,
                                        quantity=good.quantity,
                                        nomenclature=good.nomenclature_id,
                                    )
                                    for good in goods
                                ],
                                dated=datetime.datetime.now().timestamp(),
                                is_marketplace_order=True,
                                comment=comment,
                            )
                        ]
                    ),
                )

                docs_sales_id = create_result[0]["id"]
                docs_sales_ids.append(docs_sales_id)

                # выставляем delivery_info
                await create_delivery_info(
                    token=token, idx=docs_sales_id, data=data.delivery_info
                )

                # добавляем utm
                if data.utm:
                    await self.__add_utm(token, docs_sales_id, data.utm)

            # Если всё ок — success (success не затирает error, если он уже стоит)
            await self.__set_marketplace_order_status(
                data.marketplace_order_id, "success"
            )

        except Exception as e:
            # Обновляем marketplace_orders.error и пробрасываем дальше,
            # чтобы не менять семантику ack/nack сообщений.
            err_text = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            await self.__set_marketplace_order_status(
                data.marketplace_order_id, "error", err_text
            )
            raise

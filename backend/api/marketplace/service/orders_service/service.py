import uuid
from abc import ABC
from typing import List, Optional

from api.marketplace.rabbitmq.messages.CreateMarketplaceOrderMessage import (
    CreateMarketplaceOrderMessage,
    OrderGoodMessage,
)
from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.orders_service.schemas import (
    CreateOrderUtm,
    MarketplaceOrderRequest,
    MarketplaceOrderResponse,
)
from api.marketplace.service.products_list_service.schemas import AvailableWarehouse
from database.db import (
    database,
    marketplace_orders,
    nomenclature,
    organizations,
    warehouse_balances,
)
from fastapi import HTTPException
from sqlalchemy import and_, desc, select, update


class MarketplaceOrdersService(BaseMarketplaceService, ABC):
    """Сервис оформления заказа"""

    @staticmethod
    async def __set_marketplace_order_status(
        marketplace_order_id: int, status: str, error: Optional[str] = None
    ) -> None:
        values = {"status": status}
        if error is not None:
            values["error"] = error[:8000]

        stmt = update(marketplace_orders).where(
            marketplace_orders.c.id == marketplace_order_id
        )

        # Не затираем error статусом success/queued
        if status != "error":
            stmt = stmt.where(marketplace_orders.c.status != "error")

        await database.execute(stmt.values(**values))

    @staticmethod
    async def __transform_good(good: OrderGoodMessage) -> OrderGoodMessage:
        # Если organization_id не задан и склад указан - находим organization_id по балансу
        # Если склад не указан (warehouse_id = None) - organization_id остаётся -1
        if good.organization_id == -1 and good.warehouse_id is not None:
            good.organization_id = (
                await BaseMarketplaceService._get_latest_organization_id_for_balance(
                    warehouse_id=good.warehouse_id,
                    nomenclature_id=good.nomenclature_id,
                )
            )
        return good

    async def create_order(
        self, order_request: MarketplaceOrderRequest, utm: CreateOrderUtm
    ) -> MarketplaceOrderResponse:
        if not self._rabbitmq:
            raise HTTPException(status_code=500, detail="RabbitMQ не инициализирован")

        await self._ensure_marketplace_client(order_request.contragent_phone)

        ins = (
            marketplace_orders.insert()
            .values(
                phone=order_request.contragent_phone,
                delivery_info=order_request.delivery.dict(),
                additional_data=order_request.additional_data or [],
                status="created",
            )
            .returning(marketplace_orders.c.id)
        )
        row = await database.fetch_one(ins)
        if not row:
            raise HTTPException(
                status_code=500, detail="Не удалось создать marketplace_order"
            )
        marketplace_order_id: int = row.id

        goods_dict: dict[int, list[OrderGoodMessage]] = {}

        for good_req in order_request.goods:
            cashbox_query = select(nomenclature.c.cashbox).where(
                and_(
                    nomenclature.c.id == good_req.nomenclature_id,
                    nomenclature.c.is_deleted.is_not(True),
                )
            )
            cashbox_row = await database.fetch_one(cashbox_query)
            if not cashbox_row:
                await self.__set_marketplace_order_status(
                    marketplace_order_id,
                    "error",
                    f"Товар nomenclature_id={good_req.nomenclature_id} не найден",
                )
                raise HTTPException(status_code=404, detail="Товар не найден")

            cashbox_id = cashbox_row.cashbox

            good = OrderGoodMessage(
                organization_id=-1,
                **good_req.dict(),
            )

            # Если склад не задан — подбираем доступный
            # Но если складов нет, склад остаётся None (не обязателен)
            if good.warehouse_id is None:
                warehouses: List[AvailableWarehouse] = (
                    await self._fetch_available_warehouses(
                        nomenclature_id=good.nomenclature_id,
                        client_lat=order_request.client_lat,
                        client_lon=order_request.client_lon,
                    )
                )
                if warehouses:
                    # Если есть доступные склады, выбираем первый
                    selected = warehouses[0]
                    good.warehouse_id = selected.warehouse_id
                    good.organization_id = selected.organization_id
                else:
                    # Если складов нет, пытаемся найти organization_id из любого остатка
                    # для этой номенклатуры (нужно для создания документа продажи)
                    org_query = (
                        select(warehouse_balances.c.organization_id)
                        .where(
                            warehouse_balances.c.nomenclature_id == good.nomenclature_id
                        )
                        .order_by(
                            desc(warehouse_balances.c.created_at),
                            desc(warehouse_balances.c.id),
                        )
                        .limit(1)
                    )
                    org_row = await database.fetch_one(org_query)
                    if org_row:
                        good.organization_id = org_row.organization_id
                    else:
                        # Если остатков нет, берём первую организацию для cashbox
                        # (нужно для создания документа продажи)
                        default_org_query = (
                            select(organizations.c.id)
                            .where(
                                and_(
                                    organizations.c.cashbox == cashbox_id,
                                    organizations.c.is_deleted.is_not(True),
                                )
                            )
                            .limit(1)
                        )
                        default_org = await database.fetch_one(default_org_query)
                        if default_org:
                            good.organization_id = default_org.id
                        # Если и организации нет, organization_id останется -1
                        # (но это крайний случай, обычно организация должна быть)
                    # warehouse_id остаётся None - это допустимо

            # Если склад задан, но organization_id не задан — вычисляем из последнего balance
            good = await self.__transform_good(good)

            if goods_dict.get(cashbox_id):
                goods_dict[cashbox_id].append(good)
            else:
                goods_dict[cashbox_id] = [good]

        try:
            for cashbox_id, goods in goods_dict.items():
                contragent_id = await self._get_or_create_contragent_id(
                    phone=order_request.contragent_phone,
                    cashbox_id=cashbox_id,
                )

                await self._rabbitmq.publish(
                    CreateMarketplaceOrderMessage(
                        message_id=uuid.uuid4(),
                        marketplace_order_id=marketplace_order_id,
                        phone=order_request.contragent_phone,
                        cashbox_id=cashbox_id,
                        contragent_id=contragent_id,
                        goods=goods,
                        delivery_info=order_request.delivery,
                        utm=utm,
                        additional_data=order_request.additional_data,
                    ),
                    routing_key="create_marketplace_order",
                )

            # Если publish прошёл — считаем, что заказ в очереди
            await self.__set_marketplace_order_status(marketplace_order_id, "queued")

        except Exception as e:
            await self.__set_marketplace_order_status(
                marketplace_order_id, "error", f"{type(e).__name__}: {e}"
            )
            raise

        return MarketplaceOrderResponse(message="Заказ создан и отправлен на обработку")

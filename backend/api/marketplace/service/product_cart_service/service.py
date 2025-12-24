from datetime import datetime
from typing import Optional

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.orders_service.schemas import MarketplaceOrderGood
from api.marketplace.service.product_cart_service.schemas import (
    MarketplaceAddToCartRequest,
    MarketplaceCartGoodRepresentation,
    MarketplaceCartResponse,
    MarketplaceGetCartRequest,
    MarketplaceRemoveFromCartRequest,
)
from database.db import (
    database,
    marketplace_cart_goods,
    marketplace_carts,
    nomenclature,
)
from fastapi import HTTPException
from sqlalchemy import and_, delete, select, update


class MarketplaceCartService(BaseMarketplaceService):
    async def add_to_cart(
        self, request: MarketplaceAddToCartRequest
    ) -> MarketplaceCartResponse:
        """
        Добавить товар в корзину (корзина создаётся автоматически).

        Логика как в старом коде:
        1) получить/создать корзину
        2) проверить, есть ли уже такая позиция (nomenclature_id + warehouse_id)
        3) если есть — увеличить quantity, иначе вставить новую строку
        4) вернуть корзину
        """

        phone = request.contragent_phone

        await self._ensure_marketplace_client(phone)

        product_query = select(nomenclature.c.id).where(
            and_(
                nomenclature.c.id == request.good.nomenclature_id,
                nomenclature.c.is_deleted == False,
            )
        )
        product = await database.fetch_one(product_query)
        if not product:
            raise HTTPException(
                status_code=404, detail="Товар не найден или не доступен"
            )

        cart_id = await self._get_or_create_cart(phone)

        existing_item = await self._get_cart_item(
            cart_id=cart_id,
            nomenclature_id=request.good.nomenclature_id,
            warehouse_id=request.good.warehouse_id,
        )

        if existing_item:
            new_quantity = existing_item.quantity + request.good.quantity
            upd = (
                update(marketplace_cart_goods)
                .where(marketplace_cart_goods.c.id == existing_item.id)
                .values(quantity=new_quantity, updated_at=datetime.utcnow())
            )
            await database.execute(upd)
        else:
            ins = marketplace_cart_goods.insert().values(
                nomenclature_id=request.good.nomenclature_id,
                warehouse_id=request.good.warehouse_id,
                quantity=request.good.quantity,
                cart_id=cart_id,
            )
            await database.execute(ins)

        await database.execute(
            update(marketplace_carts)
            .where(marketplace_carts.c.id == cart_id)
            .values(updated_at=datetime.utcnow())
        )

        return await self.get_cart(MarketplaceGetCartRequest(contragent_phone=phone))

    async def get_cart(
        self, request: MarketplaceGetCartRequest
    ) -> MarketplaceCartResponse:
        """Получить содержимое корзины по номеру телефона."""

        phone = request.contragent_phone

        cart_query = select(marketplace_carts.c.id).where(
            marketplace_carts.c.phone == phone
        )
        cart = await database.fetch_one(cart_query)

        if not cart:
            return MarketplaceCartResponse(
                contragent_phone=phone,
                goods=[],
                total_count=0,
            )

        items_query = select(
            marketplace_cart_goods.c.id,
            marketplace_cart_goods.c.nomenclature_id,
            marketplace_cart_goods.c.warehouse_id,
            marketplace_cart_goods.c.quantity,
            marketplace_cart_goods.c.created_at,
            marketplace_cart_goods.c.updated_at,
        ).where(marketplace_cart_goods.c.cart_id == cart.id)

        items = await database.fetch_all(items_query)

        goods = [
            MarketplaceOrderGood(
                nomenclature_id=item.nomenclature_id,
                warehouse_id=item.warehouse_id,
                quantity=item.quantity,
            )
            for item in items
        ]

        return MarketplaceCartResponse(
            contragent_phone=phone,
            goods=goods,
            total_count=len(items),
        )

    async def remove_from_cart(
        self, request: MarketplaceRemoveFromCartRequest
    ) -> MarketplaceCartResponse:
        """
        Удалить товар из корзины.

        В отличие от старого кода, не полагаемся на "result == 0" от database.execute(delete),
        а сначала ищем строку — так надёжнее.
        """

        phone = request.contragent_phone

        cart = await database.fetch_one(
            select(marketplace_carts.c.id).where(marketplace_carts.c.phone == phone)
        )
        if not cart:
            raise HTTPException(status_code=404, detail="Cart not found for this phone")

        conditions = [
            marketplace_cart_goods.c.cart_id == cart.id,
            marketplace_cart_goods.c.nomenclature_id == request.nomenclature_id,
        ]
        if request.warehouse_id is not None:
            conditions.append(
                marketplace_cart_goods.c.warehouse_id == request.warehouse_id
            )
        else:
            conditions.append(marketplace_cart_goods.c.warehouse_id.is_(None))

        item = await database.fetch_one(
            select(marketplace_cart_goods.c.id).where(and_(*conditions))
        )
        if not item:
            raise HTTPException(status_code=404, detail="Item not found in cart")

        await database.execute(
            delete(marketplace_cart_goods).where(marketplace_cart_goods.c.id == item.id)
        )

        await database.execute(
            update(marketplace_carts)
            .where(marketplace_carts.c.id == cart.id)
            .values(updated_at=datetime.utcnow())
        )

        return await self.get_cart(MarketplaceGetCartRequest(contragent_phone=phone))

    @staticmethod
    async def _get_or_create_cart(phone: str) -> int:
        """Получить существующую корзину по phone или создать новую."""
        cart = await database.fetch_one(
            select(marketplace_carts.c.id).where(marketplace_carts.c.phone == phone)
        )
        if cart:
            return cart.id

        cart_id = await database.execute(marketplace_carts.insert().values(phone=phone))
        return cart_id

    @staticmethod
    async def _get_cart_item(
        cart_id: int,
        nomenclature_id: int,
        warehouse_id: Optional[int] = None,
    ) -> Optional[MarketplaceCartGoodRepresentation]:
        conditions = [
            marketplace_cart_goods.c.cart_id == cart_id,
            marketplace_cart_goods.c.nomenclature_id == nomenclature_id,
        ]

        if warehouse_id is not None:
            conditions.append(marketplace_cart_goods.c.warehouse_id == warehouse_id)
        else:
            conditions.append(marketplace_cart_goods.c.warehouse_id.is_(None))

        row = await database.fetch_one(
            select(marketplace_cart_goods).where(and_(*conditions))
        )
        if not row:
            return None

        return MarketplaceCartGoodRepresentation.from_orm(row)

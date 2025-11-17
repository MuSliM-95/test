from datetime import datetime
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import select, update, delete, and_

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.orders_service.schemas import MarketplaceOrderGood
from api.marketplace.service.product_cart_service.schemas import MarketplaceAddToCartRequest, MarketplaceCartResponse, \
    MarketplaceCartGoodRepresentation, MarketplaceRemoveFromCartRequest, MarketplaceGetCartRequest
from database.db import (
    database,
    marketplace_contragent_cart,
    marketplace_cart_goods
)


class MarketplaceCartService(BaseMarketplaceService):
    async def add_to_cart(
            self,
            request: MarketplaceAddToCartRequest
    ) -> MarketplaceCartResponse:
        """Add item to cart (creates cart if needed)"""
        await self._validate_contragent(request.contragent_phone, request.good.nomenclature_id)
        # 1. Get contragent ID
        contragent_id = await self._get_contragent_id_by_phone(request.contragent_phone)

        # 2. Get or create cart
        cart_id = await self._get_or_create_cart(contragent_id)

        # 3. Check for existing item
        existing_item = await self._get_cart_item(
            cart_id=cart_id,
            nomenclature_id=request.good.nomenclature_id,
            warehouse_id=request.good.warehouse_id
        )

        # 4. Update or insert item
        if existing_item:
            new_quantity = existing_item.quantity + request.good.quantity
            query = (
                update(marketplace_cart_goods)
                .where(marketplace_cart_goods.c.id == existing_item.id)
                .values(
                    quantity=new_quantity,
                    updated_at=datetime.utcnow()
                )
            )
            await database.execute(query)
        else:
            query = marketplace_cart_goods.insert().values(
                nomenclature_id=request.good.nomenclature_id,
                warehouse_id=request.good.warehouse_id,
                quantity=request.good.quantity,
                cart_id=cart_id
            )
            await database.execute(query)

        # 5. Return updated cart
        return await self.get_cart(MarketplaceGetCartRequest(
            contragent_phone=request.contragent_phone
        ))

    async def get_cart(
            self,
            request: MarketplaceGetCartRequest
    ) -> MarketplaceCartResponse:
        """Get all items in cart"""
        # 1. Get contragent ID
        contragent_id = await self._get_contragent_id_by_phone(request.contragent_phone)

        # 2. Get cart
        cart = await database.fetch_one(
            select(marketplace_contragent_cart.c.id)
            .where(marketplace_contragent_cart.c.contragent_id == contragent_id)
        )

        if not cart:
            return MarketplaceCartResponse(
                contragent_phone=request.contragent_phone,
                goods=[],
                total_count=0,
            )

        # 3. Get cart items
        query = (
            select(
                marketplace_cart_goods.c.id,
                marketplace_cart_goods.c.nomenclature_id,
                marketplace_cart_goods.c.warehouse_id,
                marketplace_cart_goods.c.quantity,
                marketplace_cart_goods.c.created_at,
                marketplace_cart_goods.c.updated_at
            )
            .where(marketplace_cart_goods.c.cart_id == cart.id)
        )
        items = await database.fetch_all(query)

        # 4. Calculate totals
        total_count = len(items)

        # 5. Format response
        goods = [
            MarketplaceOrderGood(
                nomenclature_id=item.nomenclature_id,
                warehouse_id=item.warehouse_id,
                quantity=item.quantity,
            )
            for item in items
        ]

        return MarketplaceCartResponse(
            contragent_phone=request.contragent_phone,
            goods=goods,
            total_count=total_count,
        )

    async def remove_from_cart(
            self,
            request: MarketplaceRemoveFromCartRequest
    ) -> MarketplaceCartResponse:
        """Remove item from cart"""
        # 1. Get contragent ID
        contragent_id = await self._get_contragent_id_by_phone(request.contragent_phone)

        # 2. Get cart ID
        cart = await database.fetch_one(
            select(marketplace_contragent_cart.c.id)
            .where(marketplace_contragent_cart.c.contragent_id == contragent_id)
        )

        if not cart:
            raise HTTPException(
                status_code=404,
                detail="Cart not found for this contragent"
            )

        # 3. Build deletion conditions
        conditions = [
            marketplace_cart_goods.c.cart_id == cart.id,
            marketplace_cart_goods.c.nomenclature_id == request.nomenclature_id
        ]

        if request.warehouse_id is not None:
            conditions.append(marketplace_cart_goods.c.warehouse_id == request.warehouse_id)
        else:
            conditions.append(marketplace_cart_goods.c.warehouse_id.is_(None))

        # 4. Delete item(s)
        delete_query = (
            delete(marketplace_cart_goods)
            .where(and_(*conditions))
        )
        result = await database.execute(delete_query)

        if result == 0:
            raise HTTPException(
                status_code=404,
                detail="Item not found in cart"
            )

        # 5. Return updated cart
        return await self.get_cart(MarketplaceGetCartRequest(
            contragent_phone=request.contragent_phone
        ))

    @staticmethod
    async def _get_or_create_cart(contragent_id: int) -> int:
        """Get existing cart or create new one"""
        # Try to get existing cart
        cart = await database.fetch_one(
            select(marketplace_contragent_cart.c.id)
            .where(marketplace_contragent_cart.c.contragent_id == contragent_id)
        )

        if cart:
            return cart.id

        # Create new cart
        query = marketplace_contragent_cart.insert().values(
            contragent_id=contragent_id
        )
        cart_id = await database.execute(query)
        return cart_id

    @staticmethod
    async def _get_cart_item(
            cart_id: int,
            nomenclature_id: int,
            warehouse_id: Optional[int] = None
    ) -> Optional[MarketplaceCartGoodRepresentation]:
        """Get specific item in cart"""
        conditions = [
            marketplace_cart_goods.c.cart_id == cart_id,
            marketplace_cart_goods.c.nomenclature_id == nomenclature_id
        ]

        if warehouse_id is not None:
            conditions.append(marketplace_cart_goods.c.warehouse_id == warehouse_id)
        else:
            conditions.append(marketplace_cart_goods.c.warehouse_id.is_(None))

        query = (
            select(marketplace_cart_goods)
            .where(and_(*conditions))
        )
        try:
            return MarketplaceCartGoodRepresentation.from_orm(await database.fetch_one(query))
        except Exception:
            return None

from fastapi import HTTPException
from sqlalchemy import and_, desc, func, select

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.favorites_service.schemas import (
    CreateFavoritesUtm,
    FavoriteListResponse,
    FavoriteRequest,
    FavoriteResponse,
)
from database.db import database, marketplace_favorites, nomenclature

ENTITY_TYPE_NOMENCLATURE = "nomenclature"


class MarketplaceFavoritesService(BaseMarketplaceService):
    async def get_favorites(
        self, contragent_phone: str, page: int, size: int
    ) -> FavoriteListResponse:
        # Для получения избранного не проверяем кешбокс: посетитель может просматривать товары
        # любых продавцов; контрагент может ещё не существовать или быть в другом кешбоксе.

        offset = (page - 1) * size

        # Fetch favorites with pagination
        favorites_query = (
            select(
                marketplace_favorites.c.id,
                marketplace_favorites.c.phone,
                marketplace_favorites.c.entity_type,
                marketplace_favorites.c.entity_id,
                marketplace_favorites.c.created_at,
                marketplace_favorites.c.updated_at,
            )
            .where(
                and_(
                    marketplace_favorites.c.phone == contragent_phone,
                    marketplace_favorites.c.entity_type == ENTITY_TYPE_NOMENCLATURE,
                )
            )
            .order_by(desc(marketplace_favorites.c.created_at))
            .limit(size)
            .offset(offset)
        )
        favorites_rows = await database.fetch_all(favorites_query)

        # Count total favorites
        count_query = (
            select(func.count())
            .select_from(marketplace_favorites)
            .where(
                and_(
                    marketplace_favorites.c.phone == contragent_phone,
                    marketplace_favorites.c.entity_type == ENTITY_TYPE_NOMENCLATURE,
                )
            )
        )
        total_count = await database.fetch_val(count_query)

        # Convert to FavoriteResponse models
        result = [
            FavoriteResponse(
                id=row.id,
                nomenclature_id=row.entity_id,  # entity_id хранит id номенклатуры
                phone=row.phone,
                created_at=row.created_at,
                updated_at=row.updated_at,
            )
            for row in favorites_rows
        ]

        return FavoriteListResponse(
            result=result, count=total_count, page=page, size=size
        )

    async def add_to_favorites(
        self, favorite_request: FavoriteRequest, utm: CreateFavoritesUtm
    ) -> FavoriteResponse:
        phone = favorite_request.contragent_phone
        nomenclature_id = favorite_request.nomenclature_id
        entity_type = ENTITY_TYPE_NOMENCLATURE
        entity_id = nomenclature_id

        # Для избранного не проверяем кешбокс: посетитель может сохранять товары
        # любых продавцов; контрагент может ещё не существовать или быть в другом кешбоксе.

        product_query = select(nomenclature.c.id).where(
            and_(
                nomenclature.c.id == nomenclature_id,
                nomenclature.c.is_deleted == False,
            )
        )
        entity = await database.fetch_one(product_query)
        if not entity:
            raise HTTPException(
                status_code=404, detail="Товар не найден или не доступен"
            )

        await self._ensure_marketplace_client(phone)

        existing_query = select(marketplace_favorites.c.id).where(
            and_(
                marketplace_favorites.c.phone == phone,
                marketplace_favorites.c.entity_type == entity_type,
                marketplace_favorites.c.entity_id == entity_id,
            )
        )
        existing_favorite = await database.fetch_one(existing_query)
        if existing_favorite:
            raise HTTPException(
                status_code=409, detail="Элемент уже добавлен в избранное"
            )

        favorite_id = await database.execute(
            marketplace_favorites.insert().values(
                phone=phone,
                entity_type=entity_type,
                entity_id=entity_id,
            )
        )

        created_favorite_query = select(
            marketplace_favorites.c.id,
            marketplace_favorites.c.phone,
            marketplace_favorites.c.entity_type,
            marketplace_favorites.c.entity_id,
            marketplace_favorites.c.created_at,
            marketplace_favorites.c.updated_at,
        ).where(marketplace_favorites.c.id == favorite_id)
        created_favorite = await database.fetch_one(created_favorite_query)

        await self._add_utm(created_favorite.id, utm)

        return FavoriteResponse(
            id=created_favorite.id,
            nomenclature_id=created_favorite.entity_id,
            phone=created_favorite.phone,
            created_at=created_favorite.created_at,
            updated_at=created_favorite.updated_at,
        )

    async def remove_from_favorites(
        self, favorite_id: int, contragent_phone: str
    ) -> None:
        # Для удаления из избранного не проверяем кешбокс: посетитель может удалять товары
        # любых продавцов; контрагент может ещё не существовать или быть в другом кешбоксе.
        # Удаляем запись только по favorite_id (phone используется только для совместимости API)
        delete_query = marketplace_favorites.delete().where(
            marketplace_favorites.c.id == favorite_id
        )
        deleted_count = await database.execute(delete_query)

        if deleted_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Запись в избранном не найдена",
            )

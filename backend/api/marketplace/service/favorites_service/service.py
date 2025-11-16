from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy import select, and_, func, desc

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.favorites_service.schemas import FavoriteRequest, FavoriteResponse, FavoriteListResponse, \
    CreateFavoritesUtm
from database.db import nomenclature, database, favorites_nomenclatures


class MarketplaceFavoritesService(BaseMarketplaceService):
    async def get_favorites(
            self,
            contragent_phone: str,
            page: int,
            size: int
    ) -> FavoriteListResponse:
        contragent_id = await self._get_contragent_id_by_phone(contragent_phone)

        offset = (page - 1) * size

        # Fetch favorites with pagination
        favorites_query = (
            select(
                favorites_nomenclatures.c.id,
                favorites_nomenclatures.c.nomenclature_id,
                favorites_nomenclatures.c.contagent_id,
                favorites_nomenclatures.c.created_at,
                favorites_nomenclatures.c.updated_at,
            )
            .where(favorites_nomenclatures.c.contagent_id == contragent_id)
            .order_by(desc(favorites_nomenclatures.c.created_at))
            .limit(size)
            .offset(offset)
        )
        favorites_rows = await database.fetch_all(favorites_query)

        # Count total favorites
        count_query = (
            select(func.count())
            .select_from(favorites_nomenclatures)
            .where(favorites_nomenclatures.c.contagent_id == contragent_id)
        )
        total_count = await database.fetch_val(count_query)

        # Convert to FavoriteResponse models
        result = [
            FavoriteResponse(
                id=row.id,
                nomenclature_id=row.nomenclature_id,
                contagent_id=row.contagent_id,
                created_at=row.created_at,
                updated_at=row.updated_at,
            )
            for row in favorites_rows
        ]

        return FavoriteListResponse(
            result=result,
            count=total_count,
            page=page,
            size=size
        )


    async def add_to_favorites(self, favorite_request: FavoriteRequest, utm: CreateFavoritesUtm) -> FavoriteResponse:
        class FavoriteNomenclatureCreate(BaseModel):
            nomenclature_id: int
            contagent_id: int

        await self._validate_contragent(favorite_request.contragent_phone, favorite_request.nomenclature_id)
        product_query = select(nomenclature.c.id).where(
            and_(
                nomenclature.c.id == favorite_request.nomenclature_id,
                nomenclature.c.is_deleted == False,  # TODO: добавить проверку на публичность price_type
            )
        )
        entity = await database.fetch_one(product_query)
        if not entity:
            raise HTTPException(status_code=404, detail="Товар не найден или не доступен")

        contragent_id = await self._get_contragent_id_by_phone(favorite_request.contragent_phone)

        existing_query = select(favorites_nomenclatures.c.id).where(
            and_(
                favorites_nomenclatures.c.nomenclature_id == favorite_request.nomenclature_id,
                favorites_nomenclatures.c.contagent_id == contragent_id,
            )
        )
        existing_favorite = await database.fetch_one(existing_query)
        if existing_favorite:
            raise HTTPException(status_code=409, detail="Элемент уже добавлен в избранное")

        insert_data = FavoriteNomenclatureCreate(nomenclature_id=favorite_request.nomenclature_id,
                                                 contagent_id=contragent_id).dict()
        favorite_id = await database.execute(favorites_nomenclatures.insert().values(**insert_data))

        created_favorite_query = select(
            favorites_nomenclatures.c.id,
            favorites_nomenclatures.c.nomenclature_id,
            favorites_nomenclatures.c.contagent_id,
            favorites_nomenclatures.c.created_at,
            favorites_nomenclatures.c.updated_at,
        ).where(favorites_nomenclatures.c.id == favorite_id)
        created_favorite = await database.fetch_one(created_favorite_query)

        # добавляем utm
        await self._add_utm(created_favorite.id, utm)

        return FavoriteResponse.from_orm(created_favorite)


    async def remove_from_favorites(self, nomenclature_id: int, contragent_phone: str) -> dict:
        """
        Удаляет запись из избранного, если она принадлежит указанному контрагенту.
        """
        await self._validate_contragent(contragent_phone, nomenclature_id)

        contragent_id = await self._get_contragent_id_by_phone(contragent_phone)

        # Проверяем, существует ли такая запись и принадлежит ли она контрагенту
        check_query = (
            select(favorites_nomenclatures.c.id)
            .join(nomenclature, nomenclature.c.id == favorites_nomenclatures.c.nomenclature_id)
            .where(
                and_(
                    nomenclature.c.id == nomenclature_id,
                    favorites_nomenclatures.c.contagent_id == contragent_id
                )
            )
        )
        existing = await database.fetch_one(check_query)
        if not existing:
            raise HTTPException(
                status_code=404,
                detail="Запись в избранном не найдена или не принадлежит указанному пользователю"
            )

        # Удаляем запись
        delete_query = (
            favorites_nomenclatures.delete()
            .where(favorites_nomenclatures.c.id == existing.id)
        )
        await database.execute(delete_query)

        return {"message": "Элемент успешно удалён из избранного"}

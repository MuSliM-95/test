import os

from typing import Optional

from fastapi import HTTPException, status
from sqlalchemy import select, update
from databases import Database

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.seller_service.schemas import (
    SellerUpdateRequest,
    SellerResponse,
)
from database.db import database, cboxes, users


class MarketplaceSellerService(BaseMarketplaceService):
    """
    Сервис для работы с профилем селлера
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def __transform_photo_route(photo_path: str) -> str:
        base_url = os.getenv("APP_URL")
        return f'https://{base_url}/api/v1/{photo_path.lstrip("/")}'

    async def update_seller_profile(
        self,
        cashbox_id: int,
        payload: SellerUpdateRequest,
        *,
        db: Database = database,
    ) -> SellerResponse:
        # 1. Нечего обновлять — сразу 400
        if (
            payload.name is None
            and payload.description is None
            and payload.photo is None
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Не передано ни одного поля для обновления",
            )

        # 2. Проверяем, что селлер существует и вытаскиваем admin_id
        cashbox = await db.fetch_one(
            select(
                cboxes.c.id,
                cboxes.c.name,
                cboxes.c.description,
                cboxes.c.admin,
            ).where(cboxes.c.id == cashbox_id)
        )

        if cashbox is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Селлер не найден",
            )

        admin_id: Optional[int] = cashbox["admin"]

        if admin_id is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="У селлера не назначен администратор",
            )

        # 3. Делаем апдейты в одной транзакции
        async with db.transaction():
            # Обновляем cashboxes
            cashbox_update_values = {}

            if payload.name is not None:
                cashbox_update_values["name"] = payload.name

            if payload.description is not None:
                cashbox_update_values["description"] = payload.description

            if cashbox_update_values:
                await db.execute(
                    cboxes.update()
                    .where(cboxes.c.id == cashbox_id)
                    .values(**cashbox_update_values)
                )

            user_update_values = {}
            if payload.photo is not None:
                user_update_values["photo"] = payload.photo

            if user_update_values:
                await db.execute(
                    users.update()
                    .where(users.c.id == admin_id)
                    .values(**user_update_values)
                )

        # 4. Читаем актуальное состояние профиля
        row = await db.fetch_one(
            select(
                cboxes.c.id.label("id"),
                cboxes.c.name.label("name"),
                cboxes.c.description.label("description"),
                users.c.photo.label("photo"),
            )
            .select_from(
                cboxes.join(users, cboxes.c.admin == users.c.id)
            )
            .where(cboxes.c.id == cashbox_id)
        )

        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Профиль селлера не найден после обновления",
            )

        data = dict(row)

        if data.get("photo"):
            data["photo"] = self.__transform_photo_route(data["photo"])

        return SellerResponse(**data)

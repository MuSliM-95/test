from typing import Union
from sqlalchemy import select, insert, update
from apps.yookassa.models.OauthBaseModel import OauthBaseModel, OauthUpdateModel
from apps.yookassa.repositories.core.IYookassaOauthRepository import IYookassaOauthRepository
from database.db import yookassa_install, database


class YookassaOauthRepository(IYookassaOauthRepository):

    async def get_oauth(self, cashbox: int, warehouse: int) -> Union[OauthBaseModel, None]:
        try:
            query = select(yookassa_install).where(
                yookassa_install.c.cashbox_id == cashbox,
                yookassa_install.c.warehouse_id == warehouse,
                yookassa_install.c.is_deleted == False
            )
            oauth = await database.fetch_one(query)
            if oauth:
                return OauthBaseModel(**oauth)
            else:
                return None
        except Exception as error:
            raise Exception(f"ошибка БД: {str(error)}")

    async def update_oauth(self, cashbox: int, warehouse: int, oauth: OauthUpdateModel):
        oauth_db_model = await self.get_oauth(cashbox, warehouse)
        update_data = oauth.dict(exclude_none = True)
        update_oauth = oauth_db_model.copy(update = update_data)
        query = update(yookassa_install).where(
            yookassa_install.c.cashbox_id == cashbox,
            yookassa_install.c.warehouse_id == warehouse,
            yookassa_install.c.is_deleted == False
        ).values(update_oauth.dict())\
            .returning(yookassa_install.c.id)

        return await database.execute(query)

    async def insert_oauth(self, cashbox: int, oauth: OauthBaseModel):
        query = insert(yookassa_install).values(oauth.dict()).returning(yookassa_install.c.id)
        return await database.execute(query)

    async def delete_oauth(self, cashbox: int, warehouse: int):
        oauth = await self.get_oauth(cashbox, warehouse)
        await self.update_oauth(cashbox, warehouse, OauthUpdateModel(**oauth.dict(), is_delete = True))

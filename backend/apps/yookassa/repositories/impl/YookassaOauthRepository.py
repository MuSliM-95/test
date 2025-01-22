from sqlalchemy import select, insert, update

from apps.yookassa.models.OauthBaseModel import OauthBaseModel, OauthModel, OauthUpdateModel
from apps.yookassa.models.OauthModelCredential import OauthModelCredential
from apps.yookassa.repositories.core.IYookassaOauthRepository import IYookassaOauthRepository
import os
from database.db import yookassa_install, database


class YookassaOauthRepository(IYookassaOauthRepository):

    async def get_oauth(self, cashbox: int) -> OauthBaseModel:
        query = select(yookassa_install).where(
            yookassa_install.c.cashbox_id == cashbox,
            yookassa_install.c.is_delete == False
        )
        oauth = await database.fetch_one(query)
        return OauthBaseModel(**oauth)

    async def update_oauth(self, cashbox: int, oauth: OauthUpdateModel):
        oauth_db_model = await self.get_oauth(cashbox)
        update_data = oauth.dict(exclude_none = True)
        update_oauth = oauth_db_model.copy(update_data = update_data)

        query = update(yookassa_install).where(
            yookassa_install.c.cashbox_id == cashbox,
            yookassa_install.c.is_delete == False
        ).values(update_oauth.dict())\
            .returning(yookassa_install.c.id)

        return await database.execute(query)

    async def insert_oauth(self, cashbox: int, oauth: OauthBaseModel):
        query = insert(yookassa_install).values(OauthBaseModel.dict()).returning(yookassa_install.c.id)
        return await database.execute(query)

    async def delete_oauth(self, cashbox: int):
        oauth = await self.get_oauth(cashbox)
        print(oauth)
        await self.update_oauth(cashbox, OauthUpdateModel(**oauth.dict(), is_delete = True))

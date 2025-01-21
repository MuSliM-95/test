from sqlalchemy import select, insert

from apps.yookassa.models.OauthBaseModel import OauthBaseModel, OauthModel, OauthUpdateModel
from apps.yookassa.models.OauthModelCredential import OauthModelCredential
from apps.yookassa.repositories.core.IOauthRepository import IOauthRepository
import os
from database.db import yookassa_install, database


class OauthRepository(IOauthRepository):

    async def get_oauth_credentials(self) -> OauthModelCredential:
        return OauthModelCredential(
            client_id = os.getenv("YOOKASSA_OAUTH_APP_CLIENT_ID"),
            client_secret = os.getenv("YOOKASSA_OAUTH_APP_CLIENT_SECRET")
        )

    async def get_oauth(self, cashbox: int) -> OauthBaseModel | None:
        query = select(yookassa_install).where(
            yookassa_install.c.cashbox_id == cashbox,
            yookassa_install.c.is_delete == False
        )
        oauth = await database.fetch_one(query)
        return OauthBaseModel(**oauth)

    async def update_oauth(self, cashbox: int, oauth: OauthUpdateModel) -> None:
        pass

    async def insert_oauth(self, cashbox: int, oauth: OauthBaseModel) -> int | None:
        query = insert(yookassa_install).values(OauthBaseModel.dict()).returning(yookassa_install.c.id)
        return await database.execute(query)

    async def delete_oauth(self, cashbox: int):
        pass

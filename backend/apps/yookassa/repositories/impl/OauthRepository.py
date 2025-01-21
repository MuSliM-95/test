from sqlalchemy import select

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

    async def get_access_token(self, cashbox: int) -> str:
        query = select(yookassa_install).where(yookassa_install.c.cashbox_id == cashbox)
        token = await database.fetch_one(query)
        return token.access_token

    async def update_access_token(self, cashbox: int) -> bool:
        pass

    async def add_oauth_cashbox(self, cashbox: int) -> None:
        pass

    async def delete_oauth_cashbox(self, cashbox: int):
        pass

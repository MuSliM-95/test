from apps.yookassa.repositories.core.IOauthRepository import IOauthRepository
from database.db import database


class OauthRepository(IOauthRepository):
    async def get_oauth_credentials(self):
        pass

    async def get_access_token(self, cashbox: int) -> str:
        pass

    async def update_access_token(self, cashbox: int) -> bool:
        pass

class IOauthRepository:

    async def get_oauth_credentials(self):
        raise NotImplementedError

    async def get_access_token(self, cashbox: int) -> str:
        raise NotImplementedError

    async def update_access_token(self, cashbox: int) -> bool:
        raise NotImplementedError


class IOauthService:
    async def create(self, cashbox: int):
        raise NotImplementedError

    async def revoke_token(self):
        raise NotImplementedError

    async def get_access_token(self, code: str, state: int):
        raise NotImplementedError

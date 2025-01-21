class IOauthService:
    async def create(self, cashbox: int):
        raise NotImplementedError

    async def revoke_token(self):
        raise NotImplementedError

class IOauthService:
    async def oauth_link(self, cashbox: int):
        raise NotImplementedError

    async def revoke_token(self):
        raise NotImplementedError

    async def get_access_token(self, code: str, state: int):
        raise NotImplementedError

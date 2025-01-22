class IYookassaRequestRepository:

    async def token(self, code: str, client_id: str, client_secret: str):
        raise NotImplementedError

    async def get_payments(self):
        raise NotImplementedError


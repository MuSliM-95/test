
from aiohttp import BasicAuth

from apps.yookassa.repositories.core.IYookassaRequestRepository import IYookassaRequestRepository
import aiohttp


class YookassaRequestRepository(IYookassaRequestRepository):

    async def token(self, code: str, client_id: str, client_secret: str):
        async with aiohttp.ClientSession(
                base_url = "https://yookassa.ru",
                auth = BasicAuth(client_id, client_secret)
        ) as http:
            async with http.post(url = "/oauth/v2/token", data = {"grant_type": "authorization_code", "code": code}) as r:
                res = await r.json()
                if res.get("error"):
                    raise Exception(res)
                return res

    async def revoke_token(self, token: str, client_id: str, client_secret: str):
        async with aiohttp.ClientSession(
                base_url = "https://yookassa.ru",
                auth = BasicAuth( client_id, client_secret)
        ) as http:
            async with http.post(url = "/oauth/v2/revoke_token") as r:
                res = await r.json()
                if res.get("error"):
                    raise Exception(res)
                return res

    async def get_payments(self):
        pass


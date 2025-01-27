import uuid

from aiohttp import BasicAuth

from apps.yookassa.models.PaymentModel import PaymentCreateModel
from apps.yookassa.repositories.core.IYookassaRequestRepository import IYookassaRequestRepository
import aiohttp
import json
from uuid import UUID


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
                auth = BasicAuth(client_id, client_secret)
        ) as http:
            async with http.post(url = "/oauth/v2/revoke_token", data={"token": token}) as r:
                res = await r.json()
                if res.get("error"):
                    raise Exception(res)
                return res

    async def create_payments(self, access_token: str, payment: PaymentCreateModel):
        payment_dict = payment.dict(exclude_none = True)
        async with aiohttp.ClientSession(
            base_url = "https://api.yookassa.ru",
            auth = BasicAuth(login = "1020107", password = access_token),
            headers = {"Content-Type": "application/json", "Idempotence-Key": str(uuid.uuid4())}
        ) as http:
            async with http.post(url = "/v3/payments", data = json.dumps(payment_dict)) as r:
                res = await r.json()
                return res

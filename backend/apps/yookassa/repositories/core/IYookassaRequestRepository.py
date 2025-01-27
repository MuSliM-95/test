from typing import Union

from apps.yookassa.models.PaymentModel import PaymentCreateModel, PaymentBaseModel


class IYookassaRequestRepository:

    async def token(self, code: str, client_id: str, client_secret: str):
        raise NotImplementedError

    async def revoke_token(self, token: str, client_id: str, client_secret: str) -> None:
        raise NotImplementedError

    async def create_payments(self, access_token: str, payment: PaymentCreateModel) -> Union[PaymentBaseModel]:
        raise NotImplementedError


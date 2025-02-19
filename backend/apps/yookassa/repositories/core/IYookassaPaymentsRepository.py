from apps.yookassa.models.PaymentModel import PaymentBaseModel


class IYookassaPaymentsRepository:

    async def insert(self, oauth_id: int, payment: PaymentBaseModel, payment_crm_id: int):
        raise NotImplementedError

    async def update(self, payment: PaymentBaseModel):
        raise NotImplementedError

    async def fetch_one(self, payment_id: str) -> PaymentBaseModel:
        raise NotImplementedError


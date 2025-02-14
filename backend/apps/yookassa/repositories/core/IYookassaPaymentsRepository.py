from apps.yookassa.models.PaymentModel import PaymentBaseModel


class IYookassaPaymentsRepository:

    async def insert(self, oauth_id: int, payment: PaymentBaseModel):
        raise NotImplementedError


from apps.yookassa.models.PaymentModel import PaymentCreateModel


class IYookassaApiService:

    async def api_create_payment(self, cashbox: int, payment: PaymentCreateModel) -> None:
        raise NotImplementedError

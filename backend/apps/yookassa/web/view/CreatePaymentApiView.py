from apps.yookassa.models.PaymentModel import PaymentCreateModel
from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService
from functions.helpers import get_user_by_token


class CreatePaymentApiView:

    def __init__(
            self,
            yookassa_api_service: IYookassaApiService
    ):
        self.__yookassa_api_service = yookassa_api_service

    async def __call__(self, token: str, payment: PaymentCreateModel):
        user = await get_user_by_token(token)
        await self.__yookassa_api_service.api_create_payment(cashbox = user.cashbox_id, payment = payment)

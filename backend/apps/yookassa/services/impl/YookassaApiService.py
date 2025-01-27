from apps.yookassa.models.PaymentModel import PaymentCreateModel
from apps.yookassa.repositories.core.IYookassaOauthRepository import IYookassaOauthRepository
from apps.yookassa.repositories.core.IYookassaRequestRepository import IYookassaRequestRepository
from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService


class YookassaApiService(IYookassaApiService):

    def __init__(
            self,
            request_repository: IYookassaRequestRepository,
            oauth_repository: IYookassaOauthRepository
    ):
        self.__request_repository = request_repository
        self.__oauth_repository = oauth_repository

    async def api_create_payment(self, cashbox: int, payment: PaymentCreateModel):
        try:
            oauth = await self.__oauth_repository.get_oauth(cashbox)
            return await self.__request_repository.create_payments(access_token = oauth.access_token, payment = payment)
        except Exception as error:
            raise Exception(f"ошибка создаия платежа: {str(error)}")

from apps.yookassa.models.PaymentModel import PaymentCreateModel, PaymentBaseModel
from apps.yookassa.models.WebhookBaseModel import WebhookBaseModel,WebhookViewModel
from apps.yookassa.repositories.core.IYookassaOauthRepository import IYookassaOauthRepository
from apps.yookassa.repositories.core.IYookassaPaymentsRepository import IYookassaPaymentsRepository
from apps.yookassa.repositories.core.IYookassaRequestRepository import IYookassaRequestRepository
from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService


class YookassaApiService(IYookassaApiService):

    def __init__(
            self,
            request_repository: IYookassaRequestRepository,
            oauth_repository: IYookassaOauthRepository,
            payments_repository: IYookassaPaymentsRepository
    ):
        self.__request_repository = request_repository
        self.__oauth_repository = oauth_repository
        self.__payments_repository = payments_repository

    async def api_create_webhook(self, cashbox: int, warehouse: int, webhook: WebhookViewModel):
        try:
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)
            response = await self.__request_repository.create_webhook(
                access_token = oauth.access_token,
                webhook = webhook
            )
            return response
        except Exception as error:
            raise Exception(f"ошибка создания webhook: {str(error)}")

    async def api_get_webhook_list(self, cashbox: int, warehouse: int) -> list[WebhookBaseModel]:
        try:
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)
            response = await self.__request_repository.get_webhook_list(
                access_token = oauth.access_token
            )
            return response
        except Exception as error:
            raise Exception(f"ошибка получения списка webhook: {str(error)}")

    async def api_delete_webhook(self, cashbox: int, warehouse: int,  webhook_id: str):
        try:
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)
            response = await self.__request_repository.delete_webhook(
                access_token = oauth.access_token,
                webhook_id = webhook_id
            )
            return response
        except Exception as error:
            raise Exception(f"ошибка удаления webhook: {str(error)}")

    async def api_create_payment(self, cashbox: int, warehouse: int, payment: PaymentCreateModel):
        try:
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)
            response = await self.__request_repository.create_payments(access_token = oauth.access_token, payment = payment)

            await self.__payments_repository.insert(oauth_id = oauth.id, payment = PaymentBaseModel(**response.dict()))
            return response
        except Exception as error:
            raise Exception(f"ошибка создания платежа: {str(error)}")

    async def api_update_payment(self, payment: PaymentBaseModel):
        try:
            return await self.__payments_repository.update(payment)
        except Exception as error:
            raise Exception(f"ошибка обновления платежа: {str(error)}")


from typing import Optional, List

from apps.yookassa.models.PaymentModel import PaymentCreateModel, PaymentBaseModel, EventWebhookPayment, ReceiptModel,\
    CustomerModel, ItemModel
from apps.yookassa.models.WebhookBaseModel import WebhookBaseModel,WebhookViewModel
from apps.yookassa.repositories.core.IYookassaCrmPaymentsRepository import IYookassaCrmPaymentsRepository
from apps.yookassa.repositories.core.IYookassaOauthRepository import IYookassaOauthRepository
from apps.yookassa.repositories.core.IYookassaPaymentsRepository import IYookassaPaymentsRepository
from apps.yookassa.repositories.core.IYookassaRequestRepository import IYookassaRequestRepository
from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService


class YookassaApiService(IYookassaApiService):

    def __init__(
            self,
            request_repository: IYookassaRequestRepository,
            oauth_repository: IYookassaOauthRepository,
            payments_repository: IYookassaPaymentsRepository,
            crm_payments_repository: IYookassaCrmPaymentsRepository,
    ):
        self.__request_repository = request_repository
        self.__oauth_repository = oauth_repository
        self.__payments_repository = payments_repository
        self.__crm_payments_repository = crm_payments_repository

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

    async def api_create_payment(
            self,
            cashbox: int,
            warehouse: int,
            doc_sales_id: Optional[int],
            payment_crm_id: Optional[int],
            payment: PaymentCreateModel,
    ):
        if doc_sales_id and (payment_crm_id is None):
            crm_payment = await self.__crm_payments_repository.get_crm_payments_by_doc_sales_id(doc_sales_id)
            payment_crm_id = crm_payment.id
        try:
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)
            if not oauth:
                raise Exception("для склада продажи не установлена интеграция с Юkassa")
            payment_db = await self.__payments_repository.fetch_one_by_crm_payment_id(payment_crm_id)

            settings = await self.__request_repository.oauth_settings(access_token = oauth.access_token)
            print(settings)
            if settings:
                payment.test = settings.test
                if settings.fiscalization:
                    if settings.fiscalization.enabled:
                        payment.receipt = ReceiptModel(customer = CustomerModel(), items = List[ItemModel])

            response = await self.__request_repository.create_payments(
                access_token = oauth.access_token,
                payment = payment
            )
            print(response)
            if not payment_db:
                await self.__payments_repository.insert(
                    oauth_id = oauth.id,
                    payment = PaymentBaseModel(**response.dict(exclude_none = True), capture = payment.capture),
                    payment_crm_id = payment_crm_id,
                )
                return response
            elif payment_db.status == EventWebhookPayment.pending:
                await self.__payments_repository.update(
                    PaymentBaseModel(**response.dict(exclude_none = True), capture = payment.capture),
                    from_webhook = False,
                    payment_id_db = payment_db.id
                )
                return response
            elif payment_db.status == EventWebhookPayment.waiting_for_capture:
                raise Exception("платеж yookassa ожидает подтверждения")
            else:
                raise Exception("платеж yookassa подтвержден и его нельзя изменить")

        except Exception as error:
            raise Exception(f"ошибка создания платежа: {str(error)}")

    async def api_update_payment(self, payment: PaymentBaseModel):
        try:
            return await self.__payments_repository.update(payment, from_webhook = True)
        except Exception as error:
            raise Exception(f"ошибка обновления платежа: {str(error)}")

    async def api_get_payment_by_docs_sales_id(self, docs_sales_id: int) -> Optional[PaymentBaseModel]:
        try:
            crm_payment = await self.__crm_payments_repository.get_crm_payments_by_doc_sales_id(docs_sales_id)
            if not crm_payment:
                raise Exception("платеж не найден по документу продажи")
            return await self.__payments_repository.fetch_one_by_crm_payment_id(crm_payment.id)
        except Exception as error:
            raise Exception(f"ошибка обновления платежа: {str(error)}")


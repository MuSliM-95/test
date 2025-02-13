from apps.yookassa.models.PaymentModel import PaymentCreateModel
from apps.yookassa.models.WebhookBaseModel import WebhookViewModel, WebhookBaseModel


class IYookassaApiService:

    async def api_create_payment(self, cashbox: int, warehouse: int, payment: PaymentCreateModel):
        raise NotImplementedError

    async def api_create_webhook(self, cashbox: int, warehouse: int, webhook: WebhookViewModel):
        raise NotImplementedError

    async def api_get_webhook_list(self, cashbox: int, warehouse: int) -> list[WebhookBaseModel]:
        raise NotImplementedError

    async def api_delete_webhook(self,cashbox: int,warehouse: int,webhook_id: str):
        raise NotImplementedError

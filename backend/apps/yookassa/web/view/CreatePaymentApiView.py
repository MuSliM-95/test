from fastapi import HTTPException

from apps.yookassa.models.PaymentModel import PaymentCreateModel
from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService
from functions.helpers import get_user_by_token


class CreatePaymentApiView:

    def __init__(
            self,
            yookassa_api_service: IYookassaApiService
    ):
        self.__yookassa_api_service = yookassa_api_service

    async def __call__(self, token: str, warehouse: int, payment: PaymentCreateModel, payment_crm_id: int = None, doc_sales_id: int = None):
        try:
            user = await get_user_by_token(token)
            payment_yookassa = await self.__yookassa_api_service.api_create_payment(
                cashbox = user.cashbox_id,
                payment_crm_id = payment_crm_id,
                doc_sales_id = doc_sales_id,
                payment = payment,
                warehouse = warehouse
            )
            return payment_yookassa
        except Exception as error:
            raise HTTPException(detail = f"Платеж не создан: {str(error)}", status_code = 432)


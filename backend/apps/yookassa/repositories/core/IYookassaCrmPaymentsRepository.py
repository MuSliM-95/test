from apps.yookassa.models.PaymentModel import PaymentBaseModel


class IYookassaCrmPaymentsRepository:

    async def get_crm_payments_by_doc_sales_id(self, doc_sales_id: int):
        raise NotImplementedError



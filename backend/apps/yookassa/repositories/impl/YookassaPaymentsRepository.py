from sqlalchemy import insert

from apps.yookassa.models.PaymentModel import PaymentBaseModel
from apps.yookassa.repositories.core.IYookassaPaymentsRepository import IYookassaPaymentsRepository
from database.db import database, yookassa_payments


class YookassaPaymentsRepository(IYookassaPaymentsRepository):

    async def insert(self, oauth_id: int, payment: PaymentBaseModel):

        query = insert(yookassa_payments).values({
            "payment_id": payment.id,
            "status": payment.status,
            "amount_value": float(payment.amount.value),
            "amount_currency": payment.amount.currency,
            "income_amount_value": float(payment.income_amount.value) if payment.income_amount else None,
            "income_amount_currency": payment.income_amount.currency if payment.income_amount else None,
            "description": payment.description,
            "is_deleted": False,
            "confirmation_url": payment.confirmation.confirmation_url

        }).returning(yookassa_payments.c.id)
        return await database.execute(query)


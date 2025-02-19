from sqlalchemy import insert,select,update

from apps.yookassa.models.PaymentModel import PaymentBaseModel,AmountModel
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

    async def fetch_one(self, payment_id: str) -> PaymentBaseModel:
        payment_db = await database.fetch_one(
            select(yookassa_payments).
            where(
                yookassa_payments.c.payment_id == payment_id
            )
        )
        return PaymentBaseModel(
            id = payment_db.payment_id,
            status = payment_db.status,
            amount = AmountModel(
                value = payment_db.amount_value,
                currency = payment_db.amount_currency,
            ),
            income_amount = AmountModel(
                value = payment_db.income_amount_value,
                currency = payment_db.income_amount_currency,
            ),
        )

    async def update(self, payment: PaymentBaseModel):
        query = update(yookassa_payments).where(
            yookassa_payments.c.payment_id == payment.id
        ).values({
            "status": payment.status,
            "income_amount_value": float(payment.income_amount.value) if payment.income_amount else None,
            "income_amount_currency": payment.income_amount.currency if payment.income_amount else None,
        })\
            .returning(yookassa_payments.c.id)

        return await database.execute(query)


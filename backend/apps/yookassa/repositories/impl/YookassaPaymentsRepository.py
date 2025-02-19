from sqlalchemy import insert,select,update

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

    async def fetch_one(self, payment_id: str) -> PaymentBaseModel:
        payment_db = await database.fetch_one(
            select(yookassa_payments).
            where(
                yookassa_payments.c.payment_id == payment_id
            )
        )
        return PaymentBaseModel(**payment_db)

    async def update(self, payment: PaymentBaseModel):
        payment_db_model = await self.fetch_one(payment.id)
        update_data = payment.dict(exclude_none = True)
        update_payment = payment_db_model.copy(update = update_data)
        query = update(yookassa_payments).where(
            yookassa_payments.c.payment_id == payment.id
        ).values(update_payment.dict())\
            .returning(yookassa_payments.c.id)

        return await database.execute(query)


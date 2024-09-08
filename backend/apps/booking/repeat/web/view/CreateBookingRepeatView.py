import uuid

from fastapi import HTTPException
from sqlalchemy import select, and_

from apps.booking.repeat.models.BaseBookingRepeatMessageModel import BaseBookingRepeatMessage
from common.amqp_messaging.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage
from database.db import docs_sales, amo_leads_docs_sales_mapping, amo_leads, database, booking
from functions.helpers import get_user_by_token


class CreateBookingRepeatView:

    def __init__(
        self,
        amqp_messaging_factory: IRabbitFactory
    ):
        self.__amqp_messaging_factory = amqp_messaging_factory

    async def __call__(
        self,
        token: str, lead_id: int
    ):
        user = await get_user_by_token(token)

        amqp_messaging = await self.__amqp_messaging_factory()

        query = (
            select(docs_sales.c.id, amo_leads.c.id)
            .select_from(docs_sales)
            .join(amo_leads_docs_sales_mapping, docs_sales.c.id == amo_leads_docs_sales_mapping.c.docs_sales_id)
            .join(amo_leads, amo_leads_docs_sales_mapping.c.lead_id == amo_leads.c.id)
            .where(and_(
                amo_leads.c.amo_id == lead_id,
                docs_sales.c.cashbox == user.cashbox_id,
                docs_sales.c.is_deleted == False,
                amo_leads.c.is_deleted == False,
            ))
        )

        docs_sales_id = await database.fetch_one(query)

        if not docs_sales_id:
            raise HTTPException(status_code=404, detail="Docs Sales Not Found")

        query = (
            select(booking)
            .where(and_(
                booking.c.docs_sales_id == docs_sales_id.id,
                booking.c.cashbox == user.cashbox_id,
                booking.c.is_deleted == False
            ))
        )
        booking_info = await database.fetch_one(query)

        if not booking_info:
            raise HTTPException(status_code=404, detail="Booking Not Found")

        await amqp_messaging.publish(
            BaseBookingRepeatMessage(
                message_id=uuid.uuid4(),
                cashbox_id=user.cashbox_id,
                booking_id=booking_info.id,
                start_booking=booking_info.start_booking,
                end_booking=booking_info.end_booking,
                token=token,
                lead_id=docs_sales_id.id_1
            ),
            routing_key="booking_repeat_tasks"
        )
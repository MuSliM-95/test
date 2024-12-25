import uuid

from fastapi import HTTPException
from sqlalchemy import select, and_
from starlette import status
from starlette.requests import Request

from apps.booking.repeat.models.BaseBookingRepeatMessageModel import BaseBookingRepeatMessage
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from database.db import docs_sales, amo_leads_docs_sales_mapping, amo_leads, database, booking, docs_sales_tags
from functions.helpers import get_user_by_token


class CreateBookingRepeatView:

    def __init__(
        self,
        amqp_messaging_factory: IRabbitFactory
    ):
        self.__amqp_messaging_factory = amqp_messaging_factory

    async def __call__(
        self,
        token: str, request: Request
    ):
        form = await request.form()
        form_dict = dict(form)

        user = await get_user_by_token(token)

        amqp_messaging: IRabbitMessaging = await self.__amqp_messaging_factory()

        lead_id = form_dict.get("leads[add][0][id]")
        if not lead_id:
            return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Lead Id Not Available")

        query = (
            select(amo_leads.c.id)
            .where(and_(
                amo_leads.c.amo_id == int(lead_id),
                amo_leads.c.is_deleted == False,
            ))
        )
        amo_lead_info = await database.fetch_one(query)
        if not amo_lead_info:
            return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lead Not Found")

        query = (
            select(docs_sales)
            .join(docs_sales_tags, docs_sales.c.id == docs_sales_tags.c.docs_sales_id)
            .where(docs_sales_tags.c.name == f"ID_{lead_id}")
        )
        synced_docs_sales = await database.fetch_one(query)
        if not synced_docs_sales:
            raise HTTPException(status_code=404, detail="Docs Sales Not Found")

        query = (
            select(booking)
            .where(and_(
                booking.c.docs_sales_id == synced_docs_sales.id,
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
                lead_id=amo_lead_info.id
            ),
            routing_key="booking_repeat_tasks"
        )

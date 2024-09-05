from pydantic.validators import datetime, timedelta
from sqlalchemy import select, and_, insert

from apps.booking.repeat.handlers.core.IBookingRepeatEvent import IBookingRepeatEvent
from apps.booking.repeat.models.BaseBookingRepeatMessageModel import BaseBookingRepeatMessage
from database.db import booking, booking_nomenclature, database, nomenclature, docs_sales, docs_sales_goods


class BookingRepeatEvent(IBookingRepeatEvent):

    def __init__(
        self,
        booking_repeat_message: BaseBookingRepeatMessage
    ):
        self.__booking_repeat_message = booking_repeat_message

    async def __call__(self):
        query = (
            select(booking_nomenclature)
            .where(
                and_(
                    booking_nomenclature.c.booking_id == self.__booking_repeat_message.booking_id,
                    booking_nomenclature.c.is_deleted == False
                )
            )
        )
        booking_nomenclature_info = await database.fetch_one(query)

        async def booking_repeat(booking_repeat_message: BaseBookingRepeatMessage):
            query = (
                select(booking)
                .where(booking.c.id == booking_repeat_message.booking_id)
            )
            booking_info = await database.fetch_one(query)

            query = (
                select(nomenclature)
                .join(booking_nomenclature, nomenclature.c.id == booking_nomenclature.c.nomenclature_id)
                .where(and_(
                    booking_nomenclature.c.booking_id == booking_repeat_message.booking_id,
                    nomenclature.c.cashbox == booking_repeat_message.cashbox_id,
                    nomenclature.c.is_deleted == False
                ))
            )
            nomenclature_info = await database.fetch_one(query)
            if nomenclature_info.type in ["resurs", "dopresurs"]:
                nomenclature_info_dict = dict(nomenclature_info)
                del nomenclature_info_dict["id"]
                query = (
                    insert(nomenclature)
                    .values(**nomenclature_info_dict)
                    .returning(nomenclature.c.id)
                )
                new_nomenclature_id = await database.fetch_one(query)
            else:
                new_nomenclature_id = nomenclature_info.id

            query = (
                select(docs_sales_goods)
                .where(and_(
                    docs_sales_goods.c.docs_sales_id == booking_info.docs_sales_id
                ))
            )
            docs_sales_good = await database.fetch_one(query)
            docs_sales_good_dict = dict(docs_sales_good)
            del docs_sales_good_dict["id"]
            docs_sales_good_dict["nomenclature"] = new_nomenclature_id

            query = (
                select(docs_sales)
                .where(and_(
                    docs_sales.c.cashbox == booking_repeat_message.cashbox_id,
                    docs_sales.c.id == booking_info.docs_sales_id
                ))
            )
            docs_sales_info = await database.fetch_one(query)

            docs_sales_info_dict = dict(docs_sales_info)
            del docs_sales_info_dict["id"]

            query = (
                insert(docs_sales)
                .values(**docs_sales_info_dict)
                .returning(docs_sales.c.id)
            )

            new_docs_sales_id = await database.fetch_one(query)

            docs_sales_good_dict["docs_sales_id"] = new_docs_sales_id

            query = (
                insert(docs_sales_goods)
                .values(**docs_sales_good_dict)
            )
            await database.execute(query)

            booking_info_dict = dict(booking_info)
            del booking_info_dict["id"]
            booking_info_dict["date_booking"] = booking_info_dict["date_booking"] + timedelta(days=30).total_seconds()
            booking_info_dict["start_booking"] = booking_info_dict["start_booking"] + timedelta(days=30).total_seconds()
            booking_info_dict["end_booking"] = booking_info_dict["end_booking"] + timedelta(days=30).total_seconds()
            booking_info_dict["docs_sales_id"] = new_docs_sales_id

            query = (
                insert(booking)
                .values(
                    **booking_info_dict
                )
                .returning(booking.c.id)
            )
            new_booking_id = await database.fetch_one(query)

            query = (
                insert(booking_nomenclature)
                .values({
                    "booking_id": new_booking_id,
                    "nomenclature_id": new_nomenclature_id,
                    "tariff": "month",
                    "is_deleted": False,
                })
            )
            await database.execute(query)

        if booking_nomenclature_info:
            booking_start_next_month = self.__booking_repeat_message.start_booking + timedelta(days=30).total_seconds()
            query = (
                select(booking)
                .join(
                    booking_nomenclature, booking.c.id == booking_nomenclature.c.booking_id
                )
                .where(
                    and_(
                        booking.c.cashbox == self.__booking_repeat_message.cashbox_id,
                        booking.c.id == self.__booking_repeat_message.booking_id,
                        booking_nomenclature.c.nomenclature_id == booking_nomenclature_info.nomenclature_id,
                        booking_nomenclature.c.is_deleted == False
                    )
                )
                .where(
                    and_(
                        booking.c.start_booking <= booking_start_next_month,
                        booking_start_next_month <= booking.c.end_booking
                    )
                )
            )
            find_booking = await database.fetch_one(query)

            if not find_booking:
                await booking_repeat()
            else:
                print("REPEAT DENIED")
        else:
            await booking_repeat()
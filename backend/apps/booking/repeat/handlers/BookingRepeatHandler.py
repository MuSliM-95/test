import uuid
from datetime import datetime
from typing import Mapping, Any

from pydantic.validators import timedelta
from sqlalchemy import select, and_, insert

from api.docs_sales.routers import create
from api.docs_sales.schemas import CreateMass, Create, Item
from apps.amocrm.leads.models.NewLeadBaseModelMessage import NewLeadBaseModelMessage
from apps.booking.repeat.models.BaseBookingRepeatMessageModel import BaseBookingRepeatMessage
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from database.db import booking, booking_nomenclature, database, nomenclature, docs_sales, docs_sales_goods, \
    entity_to_entity, payments, loyality_transactions, amo_leads, amo_contacts, amo_lead_statuses, booking_tags


class BookingRepeatEvent(IEventHandler[BaseBookingRepeatMessage]):

    def __init__(
        self,
        rabbitmq_messaging_factory: IRabbitFactory
    ):
        self.__rabbitmq_messaging_factory = rabbitmq_messaging_factory

    async def __call__(self, event: Mapping[str, Any]):
        rabbitmq_messaging: IRabbitMessaging = await self.__rabbitmq_messaging_factory()
        booking_repeat_message = BaseBookingRepeatMessage(**event)
        query = (
            select(booking_nomenclature)
            .where(
                and_(
                    booking_nomenclature.c.booking_id == booking_repeat_message.booking_id,
                    booking_nomenclature.c.is_deleted == False
                )
            )
        )
        booking_nomenclature_info = await database.fetch_one(query)

        query = (
            select(amo_leads)
            .where(amo_leads.c.id == booking_repeat_message.lead_id)
        )
        lead_info = await database.fetch_one(query)

        amo_contact_ext_id = lead_info.contact_id

        async def __calculate_paid_doc(docs_sales_id, cashbox_id):
            query = (
                select(entity_to_entity)
                .where(entity_to_entity.c.cashbox_id == cashbox_id)
                .where(entity_to_entity.c.from_id == docs_sales_id)
            )
            proxyes = await database.fetch_all(query)

            paid_rubles = 0
            paid_loyality = 0

            for proxy in proxyes:
                if proxy.from_entity == 7:

                    # Платеж

                    if proxy.to_entity == 5:
                        query = (
                            select(payments)
                            .where(and_(
                                payments.c.id == proxy.to_id,
                                payments.c.cashbox == cashbox_id,
                                payments.c.status == True,
                                payments.c.is_deleted == False
                            ))
                        )
                        payment = await database.fetch_one(query)
                        if payment:
                            paid_rubles += payment.amount

                        # Транзакция
                        if proxy.to_entity == 6:
                            query = (
                                select(loyality_transactions)
                                .where(and_(
                                    loyality_transactions.c.id == proxy.to_id,
                                    loyality_transactions.c.cashbox == cashbox_id,
                                    loyality_transactions.c.status == True,
                                    loyality_transactions.c.is_deleted == False
                                ))
                            )
                            trans = await database.fetch_one(query)
                            if trans:
                                paid_loyality += trans.amount

            return paid_rubles, paid_loyality

        async def booking_repeat(
            booking_repeat_message: BaseBookingRepeatMessage,
            rabbitmq_messaging_instance: IRabbitMessaging
        ):
            async with database.transaction(force_rollback=True):
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
                new_nomenclature_id = nomenclature_info.id
                nomenclature_name = nomenclature_info.name

                query = (
                    select(docs_sales_goods)
                    .where(and_(
                        docs_sales_goods.c.docs_sales_id == booking_info.docs_sales_id
                    ))
                )
                docs_sales_goods_info_list = await database.fetch_all(query)

                if not docs_sales_goods_info_list:
                    print("Чёт не то")
                    return

                nomenclatures_to_duplicate = []
                for good in docs_sales_goods_info_list:
                    if good.type in ["resurs", "dopresurs"]:
                        nomenclatures_to_duplicate.append(good)

                query = (
                    select(docs_sales)
                    .where(and_(
                        docs_sales.c.cashbox == booking_repeat_message.cashbox_id,
                        docs_sales.c.id == booking_info.docs_sales_id
                    ))
                )
                docs_sales_info = await database.fetch_one(query)
                docs_sales_info_dict = dict(docs_sales_info)

                paid_rubles, paid_loyality = await __calculate_paid_doc(
                    docs_sales_id=booking_info.docs_sales_id,
                    cashbox_id=booking_repeat_message.cashbox_id
                )

                del docs_sales_info_dict["number"]
                del docs_sales_info_dict["dated"]
                result = await create(
                    token=booking_repeat_message.token,
                    docs_sales_data=CreateMass(
                        __root__=[
                            Create(
                                number="1" if not docs_sales_info.number else int(docs_sales_info.number) + 1,
                                dated=datetime.now().timestamp(),
                                paid_rubles=paid_rubles,
                                paid_lt=paid_loyality,
                                **docs_sales_info_dict,
                                goods=[
                                    Item(
                                        nomenclature=docs_sales_goods_info.nomenclature,
                                        price_type=docs_sales_goods_info.price_type,
                                        price=docs_sales_goods_info.price,
                                        quantity=docs_sales_goods_info.quantity,
                                        unit=docs_sales_goods_info.unit,
                                        unit_name=docs_sales_goods_info.unit_name,
                                        tax=docs_sales_goods_info.tax,
                                        discount=docs_sales_goods_info.discount,
                                        sum_discounted=docs_sales_goods_info.sum_discounted,
                                        status=docs_sales_goods_info.status,
                                    ) for docs_sales_goods_info in nomenclatures_to_duplicate
                                ]
                            )
                        ]
                    )
                )

                booking_info_dict = dict(booking_info)
                del booking_info_dict["id"]
                del booking_info_dict["created_at"]
                del booking_info_dict["updated_at"]
                booking_info_dict["date_booking"] = int(booking_info_dict["date_booking"] + timedelta(days=30).total_seconds()) + 1
                booking_info_dict["start_booking"] = int(booking_info_dict["start_booking"] + timedelta(days=30).total_seconds()) + 1
                booking_info_dict["end_booking"] = int(booking_info_dict["end_booking"] + timedelta(days=30).total_seconds()) + 1
                booking_info_dict["docs_sales_id"] = result[0]["id"]
                print(booking_info_dict)
                query = (
                    insert(booking)
                    .values(
                        **booking_info_dict
                    )
                    .returning(booking.c.id)
                )
                new_booking_id = await database.fetch_one(query)

                query = (
                    select(booking_tags)
                    .where(booking_tags.booking_id == booking_info.id)
                )
                tags_list = await database.fetch_all(query)

                tags_for_create = dict(tags_list)
                for tag_booking in tags_for_create:
                    del tag_booking["id"]
                    tag_booking["booking_id"] = new_booking_id.id

                query = (
                    insert(booking_tags)
                    .values(**tags_for_create)
                )
                await database.execute(query)

                query = (
                    insert(booking_nomenclature)
                    .values({
                        "booking_id": new_booking_id.id,
                        "nomenclature_id": new_nomenclature_id,
                        "tariff": "month",
                        "is_deleted": False,
                    })
                )
                await database.execute(query)

                query = (
                    select(amo_lead_statuses.c.amo_id)
                    .where(amo_lead_statuses.c.id == lead_info.status_id)
                )
                status_id = await database.fetch_one(query)

                await rabbitmq_messaging_instance.publish(
                    message=NewLeadBaseModelMessage(
                        message_id=uuid.uuid4(),
                        lead_name=lead_info.name,
                        price=lead_info.price,
                        status_id=status_id.amo_id,
                        contact_ext_id=amo_contact_ext_id,
                        contact_id=lead_info.contact_id,
                        account_link="https://www.drom.ru",
                        act_link="https://www.drom.ru",
                        nomenclature=nomenclature_name,
                        start_period=booking_info_dict["start_booking"],
                        end_period=booking_info_dict["end_booking"],
                        docs_sales_id=result[0]["id"],
                        cashbox_id=booking_repeat_message.cashbox_id,
                    ),
                    routing_key="post_amo_lead"
                )

        if booking_nomenclature_info:
            booking_start_next_month = booking_repeat_message.start_booking + timedelta(days=30).total_seconds() + 1
            query = (
                select(booking)
                .join(
                    booking_nomenclature, booking.c.id == booking_nomenclature.c.booking_id
                )
                .where(
                    and_(
                        booking.c.cashbox == booking_repeat_message.cashbox_id,
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
                await booking_repeat(
                    booking_repeat_message=booking_repeat_message,
                    rabbitmq_messaging_instance=rabbitmq_messaging
                )
            else:
                print("REPEAT DENIED")
        else:
            await booking_repeat(
                booking_repeat_message=booking_repeat_message,
                rabbitmq_messaging_instance=rabbitmq_messaging
            )
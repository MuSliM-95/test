from json import loads

from pydantic import ValidationError

from apps.booking.repeat.handlers.impl.BookingRepeatEvent import BookingRepeatEvent
from apps.booking.repeat.models.BaseBookingRepeatMessageModel import BaseBookingRepeatMessage
from common.amqp_messaging.core.IRabbitFactory import IRabbitFactory


class RepeatBookingWorker:

    def __init__(
        self,
        rabbitmq_messaging_factory: IRabbitFactory
    ):
        self.__rabbitmq_messaging_factory = rabbitmq_messaging_factory

    async def start(
        self
    ):
        print("TEST0")
        rabbitmq_messaging = await self.__rabbitmq_messaging_factory()
        print("TEST1")
        async for data_bytes in rabbitmq_messaging.subscribe():
            print("TEST2")
            message_json = loads(data_bytes)
            try:
                validated_message = BaseBookingRepeatMessage(**message_json)
                print("TEST3")
                booking_repeat_event = BookingRepeatEvent(booking_repeat_message=validated_message)
                await booking_repeat_event()
            except ValidationError:
                print("Ошибка валидации")



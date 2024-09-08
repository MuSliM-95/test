from json import loads

from pydantic import ValidationError

from apps.booking.repeat.handlers.events.impl.BookingRepeatEvent import BookingRepeatEvent
from apps.booking.repeat.models.BaseBookingRepeatMessageModel import BaseBookingRepeatMessage
from common.amqp_messaging.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.core.IRabbitMessaging import IRabbitMessaging


class RepeatBookingWorker:

    def __init__(
        self,
        rabbitmq_messaging_factory: IRabbitFactory
    ):
        self.__rabbitmq_messaging_factory = rabbitmq_messaging_factory

    async def start(
        self
    ):
        rabbitmq_messaging: IRabbitMessaging = await self.__rabbitmq_messaging_factory()
        async for data_bytes in rabbitmq_messaging.subscribe(queue_name="booking_repeat_tasks"):
            message_json = loads(data_bytes)
            try:
                validated_message = BaseBookingRepeatMessage(**message_json)
                booking_repeat_event = BookingRepeatEvent(
                    booking_repeat_message=validated_message,
                    rabbitmq_messaging_factory=self.__rabbitmq_messaging_factory
                )
                await booking_repeat_event()
            except ValidationError as error:
                print(f"Ошибка валидации {error}")



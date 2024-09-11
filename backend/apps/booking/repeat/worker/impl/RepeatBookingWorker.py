from json import loads

from pydantic import ValidationError

from apps.booking.repeat.handlers.BookingRepeatHandler import BookingRepeatEvent
from apps.booking.repeat.models.BaseBookingRepeatMessageModel import BaseBookingRepeatMessage
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging


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

        booking_repeat_event = BookingRepeatEvent(
            rabbitmq_messaging_factory=self.__rabbitmq_messaging_factory
        )

        await rabbitmq_messaging.subscribe(BaseBookingRepeatMessage, booking_repeat_event)

        await rabbitmq_messaging.install(queue_name="booking_repeat_tasks")

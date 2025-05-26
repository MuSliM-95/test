import asyncio
from asyncio import create_task
from json import loads
from typing import AsyncIterable, Dict, Type

import aio_pika
from aio_pika import IncomingMessage, Message
from aio_pika.abc import AbstractQueue

from common.amqp_messaging.amqp_channels.core.IRabbitChannel import IRabbitChannel
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage
from common.amqp_messaging.models.BasePublishMessage import BasePublishMessage


class RabbitMessagingImpl(IRabbitMessaging):

    def __init__(
        self,
        channel: IRabbitChannel
    ):
        self.__channel = channel
        self.__handlers: Dict[str, IEventHandler] = {}

    async def publish(
        self,
        message: BaseModelMessage,
        routing_key: str
    ):
        publication_channel = await self.__channel.get_publication_channel()

        queue: aio_pika.abc.AbstractQueue = await publication_channel.declare_queue(
            routing_key,
            auto_delete=False,
            durable=True,
            arguments={"x-max-priority": 10}
        )

        await publication_channel.default_exchange.publish(
            Message(
                body=BasePublishMessage(
                    event_name=message.__class__.__name__,
                    event=message
                ).json().encode("utf-8"),
                content_type="application/json",
                content_encoding="utf-8",
                message_id=message.message_id.hex,
                delivery_mode=aio_pika.abc.DeliveryMode.PERSISTENT,
                app_id="TableCRM",
            ),
            routing_key=routing_key,
        )

    async def subscribe(
        self,
        event_type: Type[BaseModelMessage],
        event_handler: IEventHandler
    ):
        if self.__handlers.get(event_type.__name__):
            raise Exception(f"Handler {event_type.__name__} is already registered")

        self.__handlers[event_type.__name__] = event_handler

    async def install(
        self,
        queue_name: str,
    ):
        consumption_channel = await self.__channel.get_consumption_channel()

        queue: AbstractQueue = await consumption_channel.declare_queue(
            queue_name,
            auto_delete=False,
            durable=True,
            arguments={"x-max-priority": 10}
        )

        await queue.consume(callback=self.__amqp_event_message_consumer)

        await asyncio.Future()

    async def __amqp_event_message_consumer(self, message: IncomingMessage):
        async with message.process(ignore_processed=True):
            try:
                message_json = loads(message.body.decode("utf-8"))
            except Exception as error:
                print(f"Произошла ошибка при валидации сообщения {error}")
                return

            try:
                event_handler = self.__handlers[message_json["event_name"]]
                event = message_json["event"]
            except KeyError as error:
                print(f"Неправильный формат сообщения или указанный хендлер не найден {error}")
                return

            await create_task(event_handler(event))

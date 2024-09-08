from typing import AsyncIterable

import aio_pika

from common.amqp_messaging.core.IRabbitChannel import IRabbitChannel
from common.amqp_messaging.core.IRabbitMessaging import IRabbitMessaging
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class RabbitMessagingImpl(IRabbitMessaging):

    def __init__(
        self,
        channel: IRabbitChannel
    ):
        self.__channel = channel

    async def publish(
        self,
        message: BaseModelMessage,
        routing_key: str
    ):
        publication_channel = await self.__channel.get_publication_channel()

        print(message.json().encode("utf-8"))

        queue: aio_pika.abc.AbstractQueue = await publication_channel.declare_queue(
            "booking_repeat_tasks",
            auto_delete=False,
            durable=True
        )

        aio_pika_message = aio_pika.Message(
            body=message.json().encode("utf-8"),
            content_type="application/json",
            content_encoding="utf-8",
            message_id=message.message_id.hex,
            delivery_mode=aio_pika.abc.DeliveryMode.PERSISTENT,
            app_id="TableCRM",
        )
        await publication_channel.default_exchange.publish(
            aio_pika_message,
            routing_key=routing_key,
        )

    async def subscribe(
        self,
        queue_name: str,
    ) -> AsyncIterable[bytes]:
        consumption_channel = await self.__channel.get_consumption_channel()

        queue: aio_pika.abc.AbstractQueue = await consumption_channel.declare_queue(
            queue_name,
            auto_delete=False,
            durable=True
        )

        async with queue.iterator() as queue_iter: # no_ack=False
            async for message in queue_iter:
                async with message.process():
                    yield message.body

                    if queue.name in message.body.decode():
                        break
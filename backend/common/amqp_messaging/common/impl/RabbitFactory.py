from typing import Dict

import aio_pika
from aio_pika.abc import AbstractRobustChannel

from ...amqp_connection.impl.AmqpConnection import AmqpConnection
from ...common.core.IRabbitFactory import IRabbitFactory
from ...common.core.IRabbitMessaging import IRabbitMessaging
from ...amqp_channels.impl.RabbitChannel import RabbitChannel
from ...common.impl.RabbitMessagingImpl import RabbitMessagingImpl
from ...models.RabbitMqSettings import RabbitMqSettings


class RabbitFactory(IRabbitFactory):

    def __init__(
        self,
        settings: RabbitMqSettings
    ):
        self.__settings = settings

    async def __call__(
        self
    ) -> IRabbitFactory:

        amqp_connection = AmqpConnection(
            settings=self.__settings
        )
        try:
            await amqp_connection.install()
        except Exception as e:
            print("ошибка в install", e)
            raise e

        try:
            channels: Dict[str, AbstractRobustChannel] = {}
            channels[f"publication"] = aio_pika.abc.AbstractChannel = await amqp_connection.get_channel()
        except Exception as e:
            print('ошибка в каналах', e)
            raise e

        rabbit_channel = RabbitChannel(
            channels=channels,
            amqp_connection=amqp_connection
        )
        rabbit_messaging = RabbitMessagingImpl(channel=rabbit_channel)

        class RabbitMessageImpl(IRabbitFactory):

            async def __call__(self) -> IRabbitMessaging:
                return rabbit_messaging

        return RabbitMessageImpl()

import asyncio
from typing import Dict

import aio_pika
from aio_pika.abc import AbstractRobustChannel

from common.amqp_messaging.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.core.IRabbitMessaging import IRabbitMessaging
from common.amqp_messaging.impl.RabbitChannel import RabbitChannel
from common.amqp_messaging.impl.RabbitMessagingImpl import RabbitMessagingImpl
from common.amqp_messaging.models.RabbitMqSettings import RabbitMqSettings


class RabbitFactory(IRabbitFactory):

    def __init__(
        self,
        settings: RabbitMqSettings
    ):
        self.__settings = settings

    async def __call__(
        self
    ) -> IRabbitFactory:
        connection = await aio_pika.connect_robust(
            host=self.__settings.rabbitmq_host,
            port=self.__settings.rabbitmq_port,
            login=self.__settings.rabbitmq_user,
            password=self.__settings.rabbitmq_pass,
            virtualhost=self.__settings.rabbitmq_vhost,
            loop=asyncio.get_running_loop()
        )
        await connection.connect()

        channels: Dict[str, AbstractRobustChannel] = {}
        channels[f"publication"] = aio_pika.abc.AbstractChannel = await connection.channel(
            channel_number=len(channels) + 1)

        rabbit_channel = RabbitChannel(
            channels=channels,
            connection=connection
        )
        rabbit_messaging = RabbitMessagingImpl(channel=rabbit_channel)

        class RabbitMessageImpl(IRabbitFactory):

            async def __call__(self) -> IRabbitMessaging:
                return rabbit_messaging

        return RabbitMessageImpl()

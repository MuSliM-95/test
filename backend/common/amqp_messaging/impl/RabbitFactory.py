import asyncio
from typing import Dict

import aio_pika
from aio_pika.abc import AbstractRobustChannel

from common.amqp_messaging.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.core.IRabbitMessaging import IRabbitMessaging
from common.amqp_messaging.impl.RabbitChannel import RabbitChannel
from common.amqp_messaging.impl.RabbitMessagingImpl import RabbitMessagingImpl
from common.amqp_messaging.models.RabbitMqSettings import RabbitMqSettings
from common.amqp_messaging.models.RabbitTypeChannelEnum import RabbitTypeChannelEnum


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

        channels: Dict[RabbitTypeChannelEnum, AbstractRobustChannel] = {}
        channels[RabbitTypeChannelEnum.PUBLICATION] = aio_pika.abc.AbstractChannel = await connection.channel(
            channel_number=1)
        channels[RabbitTypeChannelEnum.CONSUMPTION] = aio_pika.abc.AbstractChannel = await connection.channel(
            channel_number=2)

        queue: aio_pika.abc.AbstractQueue = await channels[RabbitTypeChannelEnum.PUBLICATION].declare_queue(
            "booking_repeat_tasks",
            auto_delete=False,
            durable=True
        )

        rabbit_channel = RabbitChannel(channels=channels)
        rabbit_messaging = RabbitMessagingImpl(channel=rabbit_channel)

        class RabbitMessageImpl(IRabbitFactory):

            async def __call__(self) -> IRabbitMessaging:
                return rabbit_messaging

        return RabbitMessageImpl()


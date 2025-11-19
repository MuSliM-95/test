import asyncio
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

        retries = 3
        for i in range(retries):
            try:
                await amqp_connection.install()
            except Exception as e:
                if retries == 0:
                    raise Exception(f'ошибка в инсталл {e}')
                print('retry', retries)
                retries -= 1
                await asyncio.sleep(1 * retries)

        channels: Dict[str, AbstractRobustChannel] = {}
        channels[f"publication"] = await amqp_connection.get_channel()

        rabbit_channel = RabbitChannel(
            channels=channels,
            amqp_connection=amqp_connection
        )
        rabbit_messaging = RabbitMessagingImpl(channel=rabbit_channel)

        class RabbitMessageImpl(IRabbitFactory):
            """Wrapper для RabbitMessagingImpl, реализующий IRabbitFactory"""
            
            def __init__(self, messaging: IRabbitMessaging):
                self._messaging = messaging

            async def __call__(self) -> IRabbitMessaging:
                return self._messaging
            
            async def publish(self, *args, **kwargs):
                return await self._messaging.publish(*args, **kwargs)
            
            async def subscribe(self, *args, **kwargs):
                return await self._messaging.subscribe(*args, **kwargs)
            
            async def install(self, *args, **kwargs):
                return await self._messaging.install(*args, **kwargs)

        return RabbitMessageImpl(rabbit_messaging)

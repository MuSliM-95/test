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
        last_error = None
        for i in range(retries):
            try:
                await amqp_connection.install()
                break
            except Exception as e:
                last_error = e
                if i == retries - 1:
                    raise Exception(f'ошибка в инсталл после {retries} попыток: {last_error}')
                print(f'retry {i + 1}/{retries}: {e}')
                await asyncio.sleep(1 * (i + 1))

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

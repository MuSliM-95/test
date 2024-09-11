import asyncio
from typing import Dict

from aio_pika import connect_robust
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel

from ...models.RabbitMqSettings import RabbitMqSettings

class AmqpConnection:

    def __init__(
        self,
        settings: RabbitMqSettings
    ):
        self.__settings: RabbitMqSettings = settings
        self.__connection: AbstractRobustConnection | None = None

        self._channels: Dict[int, AbstractRobustChannel] = {}

    async def install(self):
        connection = await connect_robust(
            host=self.__settings.rabbitmq_host,
            port=self.__settings.rabbitmq_port,
            login=self.__settings.rabbitmq_user,
            password=self.__settings.rabbitmq_pass,
            virtualhost=self.__settings.rabbitmq_vhost,
            loop=asyncio.get_running_loop()
        )
        await connection.connect()
        self.__connection = connection

    async def get_channel(self) -> AbstractRobustChannel:
        if not self.__connection:
            raise Exception("You are not connected to AMQP. Use install().")

        channel = await self.__connection.channel(channel_number=len(self._channels) + 1)
        self._channels[len(self._channels) + 1] = channel
        return channel

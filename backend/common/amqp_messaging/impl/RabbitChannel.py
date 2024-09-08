from typing import Dict

from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection

from common.amqp_messaging.core.IRabbitChannel import IRabbitChannel


class RabbitChannel(IRabbitChannel):

    def __init__(
        self,
        channels: Dict[str, AbstractRobustChannel],
        connection: AbstractRobustConnection
    ):
        self.__channels: Dict[str, AbstractRobustChannel] = channels
        self.__connection = connection

    async def get_consumption_channel(self) -> AbstractRobustChannel:
        new_consumption_channel = await self.__connection.channel(
            channel_number=len(self.__channels) + 1)
        self.__channels[f"consumption_{len(self.__channels) + 1}"] = new_consumption_channel
        return new_consumption_channel

    async def get_publication_channel(self) -> AbstractRobustChannel:
        return self.__channels.get("publication")
from typing import Dict

from aio_pika.abc import AbstractRobustChannel

from common.amqp_messaging.core.IRabbitChannel import IRabbitChannel
from common.amqp_messaging.models.RabbitTypeChannelEnum import RabbitTypeChannelEnum


class RabbitChannel(IRabbitChannel):

    def __init__(
        self,
        channels: Dict[RabbitTypeChannelEnum, AbstractRobustChannel]
    ):
        self.__channels: Dict[RabbitTypeChannelEnum, AbstractRobustChannel] = channels

    async def get_consumption_channel(self) -> AbstractRobustChannel:
        return self.__channels.get(RabbitTypeChannelEnum.CONSUMPTION)

    async def get_publication_channel(self) -> AbstractRobustChannel:
        return self.__channels.get(RabbitTypeChannelEnum.PUBLICATION)
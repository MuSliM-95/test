from typing import Type

from common.amqp_messaging.common.core.EventHandler import IEventHandler
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class IRabbitMessaging:

    async def publish(
        self,
        message: BaseModelMessage,
        routing_key: str
    ):
        raise NotImplementedError()

    async def subscribe(
        self,
        event_type: Type[BaseModelMessage],
        event_handler: IEventHandler
    ):
        raise NotImplementedError()

    async def install(
        self,
        queue_name: str,
    ):
        raise NotImplementedError()


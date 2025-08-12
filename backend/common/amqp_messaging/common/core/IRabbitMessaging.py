from typing import Type, Optional, List

import aiormq
from aio_pika.abc import AbstractRobustChannel

from ..impl.models.QueueSettingsModel import QueueSettingsModel
from ...common.core.EventHandler import IEventHandler
from ...models.BaseModelMessage import BaseModelMessage


class IRabbitMessaging:

    async def publish(
        self,
        message: BaseModelMessage,
        routing_key: str,
        priority: int = None,
        ttl_expiration: int = None
    ) -> aiormq.abc.ConfirmationFrameType:
        raise NotImplementedError()

    async def subscribe(
        self,
        event_type: Type[BaseModelMessage],
        event_handler: IEventHandler
    ):
        raise NotImplementedError()

    async def install(
        self,
        queues_settings: List[QueueSettingsModel]
    ) -> List[AbstractRobustChannel]:
        raise NotImplementedError()


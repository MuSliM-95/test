from typing import Any, TypeVar, Generic

from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage

E = TypeVar('E', bound=BaseModelMessage)

class IEventHandler(Generic[E]):

    async def __call__(self, event: E):
        raise NotImplementedError()
from typing import TypeVar, Generic

from pydantic.main import BaseModel

from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage

E = TypeVar('E', bound=BaseModelMessage)

class BasePublishMessage(BaseModel, Generic[E]):
    event_name: str
    event: E

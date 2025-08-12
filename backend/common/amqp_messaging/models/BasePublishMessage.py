from typing import TypeVar, Generic

from pydantic import BaseModel

from .BaseModelMessage import BaseModelMessage

E = TypeVar('E', bound=BaseModelMessage)

class BasePublishMessage(BaseModel, Generic[E]):
    event_name: str
    event: E

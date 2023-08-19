from typing import Union, Optional, List

from pydantic import BaseModel
import datetime


class Event(BaseModel):
    id: int
    type: Optional[str]
    name: Optional[str]
    method: Optional[str]
    url: Optional[str]
    payload: Optional[dict]
    cashbox_id: Optional[int]
    user_id: Optional[int]
    token: Optional[str]
    ip: Optional[str]
    promoimage: Optional[str]
    promodata: Optional[dict]

    created_at: datetime.datetime
    updated_at: Union[datetime.datetime, None]


class GetEvents(BaseModel):
    result: Optional[List[Event]]
    count: int

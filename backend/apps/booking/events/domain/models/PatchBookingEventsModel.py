from typing import Optional

from pydantic import BaseModel

from database.db import BookingEventStatus


class PatchBookingEventsModel(BaseModel):
    id: int
    type: Optional[BookingEventStatus]
    value: Optional[str]
    latitude: Optional[str]
    longitude: Optional[str]

from typing import Optional

from pydantic import BaseModel

from database.db import BookingEventStatus


class BaseBookingEventModel(BaseModel):
    booking_nomenclature_id: int
    type: BookingEventStatus
    value: Optional[str]
    latitude: str
    longitude: str

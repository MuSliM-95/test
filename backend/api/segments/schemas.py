from datetime import datetime
from typing import Union, Optional

from api.segments.schema_contragents import SegmentContragentCreate, SegmentContragentData
from pydantic import BaseModel

SegmentCreate = Union[SegmentContragentCreate, ]

class Segment(BaseModel):
    id: int
    name: str
    selection_field: Optional[str] = None
    criteria: dict
    updated_at: Optional[datetime] = None
    type_of_update: str
    update_settings: dict
    status: str
    is_archived: bool

SegmentData = Union[SegmentContragentData, ]
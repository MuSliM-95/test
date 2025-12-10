from datetime import datetime
from typing import Optional, Union

from api.segments.schema_actions import SegmentActions
from api.segments.schema_base import SegmentBaseCreate
from api.segments.schema_criteria import SegmentCriteria
from api.segments.segment_result import SegmentContragentData
from pydantic import BaseModel

SegmentData = Union[SegmentContragentData,]


class Segment(BaseModel):
    id: int
    name: str
    criteria: dict
    actions: Optional[dict] = None
    updated_at: Optional[datetime] = None
    type_of_update: str
    update_settings: Optional[dict]
    status: str
    is_archived: bool


class SegmentCreate(SegmentBaseCreate):
    criteria: SegmentCriteria
    actions: Optional[SegmentActions]


class SegmentWithContragents(Segment):
    contragents_count: Optional[int] = 0
    added_contragents_count: Optional[int] = 0
    deleted_contragents_count: Optional[int] = 0
    entered_contragents_count: Optional[int] = 0
    exited_contragents_count: Optional[int] = 0

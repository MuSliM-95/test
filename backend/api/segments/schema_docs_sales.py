from pydantic import BaseModel
from typing import Optional, Literal

from api.segments.schema_base import SegmentBaseCreate, Range, DateRange


class TgNotificationsAction(BaseModel):
    trigger_on_new: bool = True
    message: str
    user_tag: str
    send_to: Optional[Literal["picker", "courier"]]


class PickerCourierSchema(BaseModel):
    assigned: Optional[bool]
    start: Optional[DateRange]
    finish: Optional[DateRange]


class DocsSalesCriteria(BaseModel):
    tag: Optional[str]
    delivery_required: Optional[bool]
    created_at: Optional[DateRange]
    picker: Optional[PickerCourierSchema]
    courier: Optional[PickerCourierSchema]


class DocsSalesActions(BaseModel):
    send_tg_notification: TgNotificationsAction


class DocsSalesSegmentCreate(SegmentBaseCreate):
    selection_field: Literal["docs_sales"]
    criteria: DocsSalesCriteria
    actions: Optional[DocsSalesActions]

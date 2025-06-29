from datetime import datetime
from pydantic import BaseModel, Field
from typing import Union, Optional, Annotated

from api.segments.schema_contragents import SegmentContragentCreate, SegmentContragentData
from api.segments.schema_docs_sales import DocsSalesSegmentCreate

SegmentCreate = Annotated[
    Union[SegmentContragentCreate, DocsSalesSegmentCreate],
    Field(discriminator="selection_field")
]

SegmentData = Union[SegmentContragentData, ]

class Segment(BaseModel):
    id: int
    name: str
    selection_field: Optional[str] = None
    criteria: dict
    actions: Optional[dict] = None
    updated_at: Optional[datetime] = None
    type_of_update: str
    update_settings: dict
    status: str
    is_archived: bool
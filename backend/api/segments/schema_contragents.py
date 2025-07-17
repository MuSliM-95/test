from datetime import datetime

from pydantic import BaseModel, validator
from typing import Optional, List, Literal

from api.segments.schema_base import SegmentBaseCreate, Range, DateRange


class PurchaseCriteria(BaseModel):
    total_amount: Optional[Range]
    count: Optional[Range]
    last_purchase_days_ago: Optional[Range]
    amount_per_check: Optional[Range]
    date_range: Optional[DateRange]
    categories: Optional[List[str]]
    nomenclatures: Optional[List[str]]

    @validator("categories", "nomenclatures", each_item=True)
    def validate_category_item(cls, v):
        if len(v) < 3:
            raise ValueError("Элемент списка должен быть не короче 3 символов")
        return v


class LoyalityCriteria(BaseModel):
    balance: Optional[Range]
    expires_in_days: Optional[Range]


class SegmentCriteria(BaseModel):
    purchases: Optional[PurchaseCriteria]
    loyality: Optional[LoyalityCriteria]
    tags: Optional[List[str]]

    class Config:
        extra = "ignore"


class AddRemoveTags(BaseModel):
    name: List[str]

    @validator("name", each_item=True)
    def validate_tag_item(cls, v):
        if len(v) < 3:
            raise ValueError("Элемент списка должен быть не короче 3 символов")
        return v


class ContragentActions(BaseModel):
    add_tags: Optional[AddRemoveTags]
    remove_tags: Optional[AddRemoveTags]


class SegmentContragentCreate(SegmentBaseCreate):
    selection_field: Literal["contragents"]
    criteria: SegmentCriteria
    actions: Optional[ContragentActions]



class Contragent(BaseModel):
    id: int
    name: Optional[str]
    phone: Optional[str]


class SegmentContragentData(BaseModel):
    id: int
    updated_at: Optional[datetime] = None
    contragents: Optional[List[Contragent]] = []
    added_contragents: Optional[List[Contragent]] = []
    deleted_contragents: Optional[List[Contragent]] = []
from datetime import datetime

from pydantic import BaseModel, validator, root_validator
from typing import Optional, List, Dict, Literal


class Range(BaseModel):
    gte: Optional[float]
    lte: Optional[float]
    eq: Optional[float]


class DateRange(BaseModel):
    gte: Optional[str]
    lte: Optional[str]

    @validator('gte', 'lte')
    def validate_date_format(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d").date()
            return v
        except ValueError:
            raise ValueError("Дата должна быть в формате YYYY-MM-DD")


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

    class Config:
        extra = "ignore"


class UpdateSettings(BaseModel):
    interval_minutes: int

    @validator('interval_minutes')
    def validate_interval_minutes(cls, v):
        if v < 5:
            raise ValueError("Интервал не может быть менее 5 минут")
        return v


class SegmentCreate(BaseModel):
    name: str
    criteria: SegmentCriteria
    type_of_update: Literal["cron", "request"]
    update_settings: Optional[UpdateSettings]
    is_archived: bool

    @root_validator
    def check_update_settings(cls, values):
        update_type = values.get("type_of_update")
        settings = values.get("update_settings")

        if update_type == "cron" and settings is None:
            raise ValueError(
                "Поле 'update_settings' обязательно при type_of_update='cron'")
        return values


class Segment(BaseModel):
    id: int
    name: str
    criteria: dict
    updated_at: Optional[datetime] = None
    type_of_update: str
    update_settings: dict
    status: str
    is_archived: bool


class Contragent(BaseModel):
    id: int
    name: Optional[str]
    phone: Optional[str]


class SegmentData(BaseModel):
    id: int
    updated_at: Optional[datetime] = None
    contragents: Optional[List[Contragent]] = []
    added_contragents: Optional[List[Contragent]] = []
    deleted_contragents: Optional[List[Contragent]] = []

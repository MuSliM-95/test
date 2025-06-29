from datetime import datetime
from typing import Optional, Literal

from pydantic import BaseModel, root_validator, validator


class UpdateSettings(BaseModel):
    interval_minutes: int

    @validator('interval_minutes')
    def validate_interval_minutes(cls, v):
        if v < 5:
            raise ValueError("Интервал не может быть менее 5 минут")
        return v


class SegmentBaseCreate(BaseModel):
    name: str
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


class Range(BaseModel):
    gte: Optional[float]
    lte: Optional[float]
    eq: Optional[float]
    is_none: Optional[bool]


class DateRange(BaseModel):
    gte: Optional[str]
    lte: Optional[str]
    gte_seconds_ago: Optional[int]
    lte_seconds_ago: Optional[int]
    is_none: Optional[bool]

    @validator('gte', 'lte')
    def validate_date_format(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d").date()
            return v
        except ValueError:
            raise ValueError("Дата должна быть в формате YYYY-MM-DD")

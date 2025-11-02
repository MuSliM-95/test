from datetime import datetime
from typing import Optional, List, Literal

from pydantic import BaseModel, validator, constr, Field, root_validator

HEXColor = constr(regex=r"^#(?:[0-9a-fA-F]{6})$")

class AddRemoveTags(BaseModel):
    name: List[str]

    @validator("name", each_item=True)
    def validate_tag_item(cls, v):
        if len(v) < 3:
            raise ValueError("Элемент списка должен быть не короче 3 символов")
        return v


class Tag(BaseModel):
    name: str
    emoji: Optional[str]
    color: Optional[HEXColor]
    description: Optional[str]


class CreateTags(BaseModel):
    tags: List[Tag]


class TimeRange(BaseModel):
    from_: str = Field(..., alias="from")
    to_: str = Field(..., alias="to")

    @root_validator(pre=True)
    def validate_time_format(cls, value):
        from_ = value.get("from")
        to_ = value.get("to")
        try:
            # Попробуем распарсить в формате ЧЧ:ММ (24-часовой)
            datetime.strptime(from_, "%H:%M")
            datetime.strptime(to_, "%H:%M")
        except ValueError:
            raise ValueError(
                "Время должно быть в формате HH:MM (например, 09:30)")
        return value


class Modulo(BaseModel):
    divisor: int
    remainder: int


class TgNotificationsConditions(BaseModel):
    time_range: Optional[TimeRange]
    weekdays: Optional[List[int]]
    month_days: Optional[List[int]]
    month_day_modulo: Optional[Modulo]

    @validator('weekdays')
    def validate_weekdays(cls, v):
        if v is None:
            return v
        if not all(1 <= day <= 7 for day in v):
            raise ValueError(
                'Weekdays must be integers between 1 and 7 (1=Monday, 7=Sunday)')
        return list(set(v))

    @validator('month_days')
    def validate_month_days(cls, v):
        if v is None:
            return v
        if not all(1 <= day <= 31 for day in v):
            raise ValueError(
                'Day of month must be integers between 1 and 31')
        return list(set(v))


class Recipient(BaseModel):
    user_tag: str
    shift_status: Optional[Literal["on_shift", "off_shift", "on_break"]]
    conditions: Optional[TgNotificationsConditions]


class TgNotificationsAction(BaseModel):
    trigger_on_new: bool = True
    message: str
    user_tag: Optional[str]
    shift_status: Optional[Literal["on_shift", "off_shift", "on_break"]]
    send_to: Optional[Literal["picker", "courier"]]
    recipients: Optional[List[Recipient]]


class DocsSalesTags(BaseModel):
    tags: List[str]

    @validator("tags", each_item=True)
    def validate_tag_item(cls, v):
        if len(v) < 3:
            raise ValueError("Элемент списка должен быть не короче 3 символов")
        return v


class WaNotification(BaseModel):
    trigger_on_new: bool = True
    message: str
    wappi_token: str
    wappi_profile_id: str
    sleep: int = 5


class HttpRequest(BaseModel):
    trigger_on_new: bool = True
    method: Literal["GET", "POST", "PATCH"]
    url: str
    headers: Optional[dict]
    params: Optional[dict]
    body: Optional[dict]
    sleep: int = 5


class TransformLoyalityCard(BaseModel):
    cashback_percent: Optional[float]
    max_withdraw_percentage: Optional[float]
    lifetime: Optional[int]
    tag: Optional[str]
    apple_wallet_advertisement: Optional[str]

class AddRemoveTransaction(BaseModel):
    amount: float
    direction: Literal["plus", "minus"]
    comment: Optional[str]


class SegmentActions(BaseModel):
    add_existed_tags: Optional[AddRemoveTags]
    remove_tags: Optional[AddRemoveTags]
    client_tags: Optional[CreateTags]
    send_tg_notification: Optional[TgNotificationsAction]
    add_docs_sales_tags: Optional[DocsSalesTags]
    remove_docs_sales_tags: Optional[DocsSalesTags]
    transform_loyality_card: Optional[TransformLoyalityCard]
    add_loyality_transaction: Optional[AddRemoveTransaction]
    send_wa_notification: Optional[WaNotification]
    do_http_request: Optional[HttpRequest]


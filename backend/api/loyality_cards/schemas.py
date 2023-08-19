from pydantic import BaseModel
from typing import Optional,  List

class LoyalityCardFilters(BaseModel):
    card_number: Optional[int]
    balance: Optional[int]
    tags: Optional[str]
    income: Optional[int]
    outcome: Optional[int]
    cashback_percent: Optional[int]
    minimal_checque_amount: Optional[int]
    max_percentage: Optional[int]
    created_by_id: Optional[int]

    start_period_from: Optional[int]
    end_period_from: Optional[int]

    start_period_to: Optional[int]
    end_period_to: Optional[int]

    created_at_from: Optional[int]
    created_at_to: Optional[int]

    contragent_name: Optional[str]
    phone_number: Optional[int]
    organization_name: Optional[str]

    status_card: Optional[bool]


class LoyalityCardCreate(BaseModel):
    # id: int
    card_number: Optional[int]
    tags: Optional[str]
    # balance: Optional[int]
    # income: Optional[int]
    # outcome: Optional[int]
    phone_number: Optional[int]
    contragent_id: Optional[int]
    contragent_name: Optional[str]
    organization_id: Optional[int]
    # cashback_percent: Optional[int]
    # minimal_checque_amount: Optional[int]
    # max_withdraw_percentage: Optional[int]
    # start_period: Optional[int]
    # end_period: Optional[int]
    # max_percentage: Optional[int]
    status_card: bool = True
    is_deleted: bool = False

    class Config:
        orm_mode = True


class LoyalityCard(LoyalityCardCreate):
    id: int
    created_at: int
    updated_at: int
    data: dict


class LoyalityCardEdit(BaseModel):
    card_number: Optional[int]
    tags: Optional[str]
    cashback_percent: Optional[int]
    minimal_checque_amount: Optional[int]
    start_period: Optional[int]
    end_period: Optional[int]
    max_percentage: Optional[int]
    max_withdraw_percentage: Optional[int]
    status_card: Optional[bool]
    is_deleted: Optional[bool]


class LoyalityCardGet(BaseModel):
    id: int
    card_number: int
    tags: Optional[str]
    balance: int
    income: int
    outcome: int
    contragent_id: int
    organization_id: int
    contragent: str
    organization: str
    cashback_percent: int
    minimal_checque_amount: int
    max_withdraw_percentage: int
    start_period: int
    end_period: int
    max_percentage: int
    status_card: bool
    is_deleted: bool
    created_at: int
    updated_at: int

    class Config:
        orm_mode = True

class LoyalityCardCreateMass(BaseModel):
    __root__: List[LoyalityCardCreate]

    class Config:
        orm_mode = True

class LoyalityCardsListGet(BaseModel):
    __root__: Optional[List[LoyalityCardGet]]

    class Config:
        orm_mode = True

class LoyalityCardsList(BaseModel):
    __root__: Optional[List[LoyalityCard]]

    class Config:
        orm_mode = True

class CountRes(BaseModel):
    result: Optional[LoyalityCardsListGet]
    count: int
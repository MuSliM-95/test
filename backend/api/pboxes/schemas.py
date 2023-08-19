from pydantic import BaseModel
from typing import Optional, List

class Payboxes(BaseModel):
    id: int
    external_id: Optional[str]
    name: str
    start_balance: float
    balance: float
    balance_date: int
    created_at: int
    update_start_balance: int
    update_start_balance_date: int
    organization_id: Optional[int]
    created_at: int
    updated_at: int


    class Config:
        orm_mode = True


class PayboxesEdit(BaseModel):
    id: int
    external_id: Optional[str]
    start_balance: Optional[float]
    balance_date: Optional[int]
    name: Optional[str]
    organization_id: Optional[int]

    class Config:
        orm_mode = True


class PayboxesCreate(BaseModel):
    name: str
    start_balance: float
    external_id: Optional[str]
    organization_id: Optional[int]

    class Config:
        orm_mode = True


class GetPayments(BaseModel):
    result: Optional[List[Payboxes]]
    count: int

    class Config:
        orm_mode = True
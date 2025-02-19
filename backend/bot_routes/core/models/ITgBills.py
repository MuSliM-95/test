from datetime import datetime
from enum import Enum
from typing import Optional, List

from pydantic import BaseModel, Field

from database.db import TgBillStatus

class ITgBillsBase(BaseModel):
    payment_date: Optional[datetime] = None
    created_by: int
    s3_url: str
    plain_text: str
    file_name: str
    status: TgBillStatus
    payment_amount: Optional[float] = None
    counterparty_account_number: Optional[str] = None
    payment_purpose: Optional[str] = None
    counterparty_bank_bic: Optional[str] = None
    counterparty_name: Optional[str] = None

    tochka_bank_account_id: Optional[int] = None

class ITgBillsCreate(ITgBillsBase):
    pass

class ITgBillsUpdate(ITgBillsBase):
    payment_date: Optional[datetime] = None
    created_by: Optional[int] = None
    s3_url: Optional[str] = None
    plain_text: Optional[str] = None
    file_name: Optional[str] = None
    status: Optional[TgBillStatus] = None
    payment_amount: Optional[float] = None
    counterparty_account_number: Optional[str] = None
    payment_purpose: Optional[str] = None
    counterparty_bank_bic: Optional[str] = None
    counterparty_name: Optional[str] = None
    tochka_bank_account_id: Optional[int] = None


class ITgBillsInDBBase(ITgBillsBase):
    id: int
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None

    class Config:
        orm_mode = True

class ITgBills(ITgBillsInDBBase):
    pass


class ITgBillsExtended(ITgBills): 
    accountId: Optional[str] = None 
    request_id: Optional[str] = None




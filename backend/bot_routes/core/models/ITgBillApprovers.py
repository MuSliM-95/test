from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from database.db import TgBillApproveStatus



class ITgBillApproversBase(BaseModel):
    approver_id: int
    bill_id: int
    status: TgBillApproveStatus

class ITgBillApproversCreate(ITgBillApproversBase):
    pass
  
class ITgBillApproversUpdate(ITgBillApproversBase):
    approver_id: Optional[int] = None
    bill_id: Optional[int] = None
    status: Optional[TgBillApproveStatus] = None

class ITgBillApproversInDBBase(ITgBillApproversBase):
    id: int
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None

    class Config:
        orm_mode = True

class ITgBillApprovers(ITgBillApproversInDBBase):
    pass


class ITgBillApproversExtended(ITgBillApprovers):
    username: str


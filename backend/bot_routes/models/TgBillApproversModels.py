from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from database.db import TgBillApproveStatus



class TgBillApproversBaseModel(BaseModel):
    approver_id: int
    bill_id: int
    status: TgBillApproveStatus

class TgBillApproversCreateModel(TgBillApproversBaseModel):
    pass
  
class TgBillApproversUpdateModel(TgBillApproversBaseModel):
    approver_id: Optional[int] = None
    bill_id: Optional[int] = None
    status: Optional[TgBillApproveStatus] = None

class TgBillApproversInDBBaseModel(TgBillApproversBaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None

    class Config:
        orm_mode = True

class TgBillApproversModel(TgBillApproversInDBBaseModel):
    pass


class TgBillApproversExtendedModel(TgBillApproversModel):
    username: str


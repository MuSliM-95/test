from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class CashboxSettingsView(BaseModel):
    cashbox_id: int
    require_photo_for_writeoff: bool
    created_at: datetime
    updated_at: datetime
    is_deleted: bool

    class Config:
        orm_mode = True


class CreateCashboxSettings(BaseModel):
    require_photo_for_writeoff: bool = False


class PatchCashboxSettings(BaseModel):
    require_photo_for_writeoff: Optional[bool] = None
    is_deleted: Optional[bool] = None
from enum import Enum

from pydantic import BaseModel
from typing import Optional, List


class CreateSetting(BaseModel):
    organization: Optional[int]
    tags: Optional[str]
    cashback_percent: Optional[int] = 0
    minimal_checque_amount: Optional[int] = 0
    start_period: Optional[int] = 0
    end_period: Optional[int] = 0
    max_withdraw_percentage: Optional[int] = 0
    max_percentage: Optional[int] = 0
    lifetime: Optional[int] = 0 # lifetime in seconds

class EditSetting(BaseModel):
    organization: Optional[int]
    tags: Optional[str]
    cashback_percent: Optional[int] = 0
    minimal_checque_amount: Optional[int] = 0
    start_period: Optional[int] = 0
    end_period: Optional[int] = 0
    max_withdraw_percentage: Optional[int] = 0
    max_percentage: Optional[int] = 0
    lifetime: Optional[int] = 0 # lifetime in seconds
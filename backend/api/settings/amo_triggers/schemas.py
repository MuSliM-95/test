from pydantic import BaseModel
from typing import Optional, List

from database.enums import TriggerType, TriggerTime


class Filtersamobot(BaseModel):
    name: str = None


class CreateTrigger(BaseModel):
    name: str
    amo_bots_id: int
    type: TriggerType
    time_variant: TriggerTime
    time: int
    active: bool = False

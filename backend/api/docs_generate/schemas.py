from pydantic import BaseModel
from typing import Optional, Dict


class VariableType(BaseModel):
    result: Optional[Dict]

    class Config:
        orm_mode = True
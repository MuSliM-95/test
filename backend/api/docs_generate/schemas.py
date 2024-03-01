from enum import Enum

from pydantic import BaseModel
from typing import Optional, Dict


class TypeDoc(str, Enum):
    html = "html"
    pdf = "pdf"


class VariableType(BaseModel):
    result: Optional[Dict]

    class Config:
        orm_mode = True

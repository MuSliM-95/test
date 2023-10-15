from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class VariableType(BaseModel):
    result: Optional[Dict]
    tags: Optional[str]

    class Config:
        orm_mode = True
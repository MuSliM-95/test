from pydantic import BaseModel
from typing import List

class AutosuggestResponse(BaseModel):
    suggestions: List[str]

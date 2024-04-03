from pydantic import BaseModel
from typing import Optional, List

class ReportData(BaseModel):
    paybox: Optional[list[int]]
    datefrom: Optional[int]
    dateto: Optional[int]
    user: Optional[int] = None

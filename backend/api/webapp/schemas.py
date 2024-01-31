from pydantic import BaseModel
from typing import Optional, List


class NomenclatureCreate(BaseModel):
    name: str
    type: Optional[str]
    description_short: Optional[str]
    description_long: Optional[str]
    code: Optional[int]
    unit: Optional[int]
    category: Optional[int]
    manufacturer: Optional[int]
    pictures: Optional[List[Item]]


class NomenclatureListGetRes(BaseModel):
    result: Optional[List[NomenclatureCreate]]
    count: int

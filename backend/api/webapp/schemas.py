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

    class Config:
        orm_mode = True


class NomenclatureGet(NomenclatureCreate):
    id: int
    unit_name: Optional[str]
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class NomenclatureListGetRes(BaseModel):
    result: Optional[List[NomenclatureGet]]
    count: int

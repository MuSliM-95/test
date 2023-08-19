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

    class Config:
        orm_mode = True


class NomenclatureCreateMass(BaseModel):
    __root__: List[NomenclatureCreate]

    class Config:
        orm_mode = True


class NomenclatureEdit(NomenclatureCreate):
    name: Optional[str]


class Nomenclature(NomenclatureCreate):
    id: int
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class NomenclatureGet(NomenclatureCreate):
    id: int
    unit_name: Optional[str]
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class NomenclatureList(BaseModel):
    __root__: Optional[List[Nomenclature]]

    class Config:
        orm_mode = True

class NomenclatureListGet(BaseModel):
    __root__: Optional[List[NomenclatureGet]]

    class Config:
        orm_mode = True
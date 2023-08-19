from pydantic import BaseModel
from typing import Optional, List


class PriceTypeCreate(BaseModel):
    name: str

    class Config:
        orm_mode = True


class PriceTypeEdit(PriceTypeCreate):
    name: Optional[str]


class PriceType(PriceTypeCreate):
    id: int
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class PriceTypeList(BaseModel):
    __root__: Optional[List[PriceType]]

    class Config:
        orm_mode = True

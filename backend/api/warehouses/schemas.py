from pydantic import BaseModel
from typing import Optional, List


class WarehouseCreate(BaseModel):
    name: str
    type: Optional[str]
    description: Optional[str]
    address: Optional[str]
    phone: Optional[str]
    parent: Optional[int]
    status: bool = True

    class Config:
        orm_mode = True


class WarehouseCreateMass(BaseModel):
    __root__: List[WarehouseCreate]

    class Config:
        orm_mode = True


class WarehouseEdit(WarehouseCreate):
    name: Optional[str]
    status: Optional[bool]


class Warehouse(WarehouseCreate):
    id: int
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class WarehouseList(BaseModel):
    __root__: Optional[List[Warehouse]]

    class Config:
        orm_mode = True

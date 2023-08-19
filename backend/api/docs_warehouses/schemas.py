from enum import Enum

from pydantic import BaseModel
from typing import Optional, List


class WarehouseOperations(str, Enum):
    internal_consumption = "Внутреннее потребление"
    surplus_posting = "Оприходование излишков"
    movement = "Перемещение"
    write_off = "Списание"


class Item(BaseModel):
    price_type: Optional[int]
    price: float
    quantity: int
    unit: Optional[int]
    nomenclature: int


class Create(BaseModel):
    number: Optional[str]
    dated: Optional[int]
    operation: Optional[WarehouseOperations]
    comment: Optional[str]
    organization: int
    warehouse: Optional[int]
    goods: Optional[List[Item]]

    class Config:
        orm_mode = True


class Edit(Create):
    id: int

    class Config:
        orm_mode = True


class EditMass(BaseModel):
    __root__: List[Edit]

    class Config:
        orm_mode = True


class CreateMass(BaseModel):
    __root__: List[Create]

    class Config:
        orm_mode = True


class ViewInList(BaseModel):
    id: int
    number: Optional[str]
    dated: Optional[int]
    operation: Optional[str]
    comment: Optional[str]
    organization: int
    warehouse: Optional[int]
    sum: Optional[float]
    updated_at: int
    created_at: int


class View(ViewInList):
    goods: Optional[List[Item]]

    class Config:
        orm_mode = True


class ListView(BaseModel):
    __root__: Optional[List[ViewInList]]

    class Config:
        orm_mode = True

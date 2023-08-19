from enum import Enum

from pydantic import BaseModel
from typing import Optional, List


class Item(BaseModel):
    price_type: Optional[int]
    price: float
    quantity: int
    unit: Optional[int]
    unit_name: Optional[str]
    tax: Optional[float]
    discount: Optional[float]
    sum_discounted: Optional[float]
    status: Optional[str]
    nomenclature: int
    nomenclature_name: Optional[str]


class SaleOperations(str, Enum):
    order = "Заказ"
    realization = "Реализация"


class Create(BaseModel):
    number: Optional[str]
    dated: Optional[int]
    operation: Optional[SaleOperations]
    tags: Optional[str]
    docs_sales: Optional[int]
    comment: Optional[str]
    client: Optional[int]
    contragent: Optional[int]
    contract: Optional[int]
    organization: int
    loyality_card_id: Optional[int]
    warehouse: Optional[int]
    tax_included: Optional[bool]
    tax_active: Optional[bool]
    sales_manager: Optional[int]
    paid_rubles: Optional[float]
    paid_lt: Optional[float]
    status: Optional[bool]
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
    tags: Optional[str]
    docs_sales: Optional[int]
    nomenclature_count: Optional[int]
    paid_doc: Optional[float]
    paid_rubles: Optional[float]
    paid_loyality: Optional[float]
    status: Optional[bool]
    doc_discount: Optional[float]
    comment: Optional[str]
    client: Optional[int]
    contragent: Optional[int]
    contract: Optional[int]
    organization: int
    warehouse: Optional[int]
    sum: Optional[float]
    tax_included: Optional[bool]
    tax_active: Optional[bool]
    sales_manager: Optional[int]
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


class CountRes(BaseModel):
    result: Optional[List[ViewInList]]
    count: int


class FilterSchema(BaseModel):
    tags: Optional[str]

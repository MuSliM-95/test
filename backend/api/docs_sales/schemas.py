from enum import Enum

from pydantic import BaseModel
from typing import Optional, List, Union

from database.enums import Repeatability
from sqlalchemy import JSON


class Item(BaseModel):
    price_type: Optional[int]
    price: float
    quantity: float
    unit: Optional[int]
    unit_name: Optional[str]
    tax: Optional[float]
    discount: Optional[float]
    sum_discounted: Optional[float]
    status: Optional[str]
    nomenclature: Union[str, int]
    nomenclature_name: Optional[str]


class SaleOperations(str, Enum):
    order = "Заказ"
    realization = "Реализация"


class Settings(BaseModel):
    repeatability_period: Optional[Repeatability] = Repeatability.minutes.value
    repeatability_value: Optional[int] = 0
    date_next_created: Optional[int] = 0
    transfer_from_weekends: Optional[bool] = True
    skip_current_month: Optional[bool] = True
    repeatability_count: Optional[int] = 0
    default_payment_status: Optional[bool] = False
    repeatability_tags: Optional[bool] = False
    repeatability_status: Optional[bool] = True

    class Config:
        orm_mode = True


class Create(BaseModel):
    number: Optional[str]
    dated: Optional[int]
    operation: Optional[SaleOperations]
    tags: Optional[str] = ""
    parent_docs_sales: Optional[int]
    comment: Optional[str]
    client: Optional[int]
    contragent: Optional[int]
    contract: Optional[int]
    organization: int
    loyality_card_id: Optional[int]
    warehouse: Optional[int]
    paybox: Optional[int]
    tax_included: Optional[bool]
    tax_active: Optional[bool]
    settings: Optional[Settings]
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

class RecipientInfoSchema(BaseModel):
    name: Optional[str]
    surname: Optional[str]
    phone: Optional[str]


class DeliveryInfoSchema(BaseModel):
    address: Optional[str]
    delivery_date: Optional[int]
    recipient: Optional[RecipientInfoSchema]
    note: Optional[str]


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
    contragent_name: Optional[str]
    contract: Optional[int]
    organization: Optional[int]
    warehouse: Optional[int]
    autorepeat: Optional[bool]
    settings: Optional[Settings]
    sum: Optional[float]
    tax_included: Optional[bool]
    tax_active: Optional[bool]
    sales_manager: Optional[int]
    goods: Optional[List[Item]]
    delivery_info: Optional[DeliveryInfoSchema]
    updated_at: int
    created_at: int


class ViewInListResult(BaseModel):
    result: List[ViewInList]
    count: int


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
    operation: Optional[str]
    comment: Optional[str]

    contragent: Optional[int]
    organization: Optional[int]
    warehouse: Optional[int]
    sales_manager: Optional[int]
    created_by: Optional[int]

    status: Optional[bool]
    is_deleted: Optional[bool]

    dated_from: Optional[int]
    dated_to: Optional[int]
    created_at_from: Optional[int]
    created_at_to: Optional[int]
    updated_at_from: Optional[int]
    updated_at_to: Optional[int]

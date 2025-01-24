from pydantic import BaseModel, Field, BeforeValidator
from typing import List, Optional, Union, Annotated


class AmountModel(BaseModel):
    value: str
    currency: str = "RUB"


class CustomerModel(BaseModel):
    full_name: str = None
    inn: str = Field(None, max_length = 12, min_length = 10)
    email: str = None
    phone: str = None


class MarkQuantityModel(BaseModel):
    numerator: int
    denominator: int


class ItemModel(BaseModel):
    description: str
    amount: AmountModel
    vat_code: int
    quantity: float
    measure: str = None
    mark_quantity: MarkQuantityModel = None


class ReceiptModel(BaseModel):
    customer: CustomerModel = None
    items: List[ItemModel]


class PaymentCreateModel(BaseModel):
    amount: AmountModel
    description: str = None
    receipt: ReceiptModel = None
    tax_system_code: int = None
    capture: bool = None
    merchant_customer_id: str = None


class PaymentBaseModel(PaymentCreateModel):
    id: str
    status: str






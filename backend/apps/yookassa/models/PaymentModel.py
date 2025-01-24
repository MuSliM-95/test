from datetime import datetime

from pydantic import BaseModel, Field
from typing import List


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


class RecipientModel(BaseModel):
    account_id: str
    gateway_id: str


class InvoiceDetails(BaseModel):
    id: str = None


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
    amount: AmountModel
    income_amount: AmountModel
    description: str = None
    recipient: RecipientModel
    captured_at: datetime = None
    created_at: datetime
    expires_at: datetime = None
    test: bool
    refundable: bool
    receipt_registration: str
    merchant_customer_id: str = None
    invoice_details: InvoiceDetails = None






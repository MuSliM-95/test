from pydantic import BaseModel, Field
from typing import Optional

import datetime
from enum import Enum


class Contragent_types(str, Enum):
    Supplier = "Поставщик"
    Buyer = "Покупатель"


class Contragent(BaseModel):
    name: str
    phone: Optional[str]
    inn: Optional[str]
    description: Optional[str]

    contragent_type: Contragent_types = Field(None, alias='Contragent_types')
    birth_date: datetime.datetime
    data: Optional[dict]


class ContragentEdit(BaseModel):
    name: Optional[str]
    external_id: Optional[str]
    phone: Optional[str]
    inn: Optional[str]
    description: Optional[str]

    contragent_type: Contragent_types = Field(None, alias='Contragent_types')
    birth_date: datetime.datetime
    data: Optional[dict]


class ContragentCreate(BaseModel):
    name: str
    external_id: Optional[str]
    phone: Optional[str]
    inn: Optional[str]
    description: Optional[str]

    contragent_type: Contragent_types = Field(None, alias='Contragent_types')
    birth_date: datetime.datetime
    data: Optional[dict]
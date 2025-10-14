from pydantic import BaseModel, Field
from typing import Optional

from enum import Enum
from datetime import date
from database.enums import Gender, ContragentType

class Contragent_types(str, Enum):
    Supplier = "Поставщик"
    Buyer = "Покупатель"


class Contragent(BaseModel):
    name: str
    phone: Optional[str]
    inn: Optional[str]
    description: Optional[str]

    contragent_type: Optional[Contragent_types]
    birth_date: Optional[date]
    data: Optional[dict]
    gender: Optional[Gender] = None
    type: Optional[ContragentType] = None
    additional_phones: Optional[str]


class ContragentEdit(BaseModel):
    name: Optional[str]
    external_id: Optional[str]
    phone: Optional[str]
    inn: Optional[str]
    description: Optional[str]

    contragent_type: Optional[Contragent_types]
    birth_date: Optional[date]
    data: Optional[dict]
    gender: Optional[Gender] = None
    type: Optional[ContragentType] = None
    additional_phones: Optional[str]


class ContragentCreate(BaseModel):
    name: str
    external_id: Optional[str]
    phone: Optional[str]
    inn: Optional[str]
    description: Optional[str]

    contragent_type: Optional[Contragent_types]
    birth_date: Optional[date]
    email: Optional[str]
    data: Optional[dict]
    gender: Optional[Gender] = None
    type: Optional[ContragentType] = None
    additional_phones: Optional[str]

class ContragentResponse(Contragent):
    external_id: Optional[str]

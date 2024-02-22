from pydantic import BaseModel
from typing import Optional


class Contragent(BaseModel):
    name: str
    phone: Optional[str]
    inn: Optional[str]
    description: Optional[str]


class ContragentEdit(BaseModel):
    name: Optional[str]
    external_id: Optional[str]
    phone: Optional[str]
    inn: Optional[str]
    description: Optional[str]


class ContragentCreate(BaseModel):
    name: str
    external_id: Optional[str]
    phone: Optional[str]
    inn: Optional[str]
    description: Optional[str]
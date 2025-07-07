from typing import List, Optional
from enum import Enum
from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime


class TechCardType(str, Enum):
    reference = "reference"
    automatic = "automatic"


class TechCardStatus(str, Enum):
    active = "active"
    canceled = "canceled"
    deleted = "deleted"


class TechCardBase(BaseModel):
    name: str = Field(..., min_length=1)
    description: Optional[str] = None
    card_type: TechCardType
    auto_produce: bool = False


class TechCard(TechCardBase):
    id: UUID
    created_at: datetime
    updated_at: datetime
    user_id: int
    status: TechCardStatus

    class Config:
        orm_mode = True


class TechCardItemCreate(BaseModel):
    name: str = Field(..., min_length=1)
    quantity: float = Field(None, gt=0)
    gross_weight: Optional[float] = Field(None, gt=0)
    net_weight: Optional[float] = Field(None, gt=0)
    nomenclature_id: int


class TechCardCreate(TechCardBase):
    items: List[TechCardItemCreate]


class TechCardItem(TechCardItemCreate):
    id: UUID
    tech_card_id: UUID

    class Config:
        orm_mode = True


class TechCardResponse(TechCard):
    items: List[TechCardItem]

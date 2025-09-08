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
    # name: str = Field(..., min_length=1)
    nomenclature_id: int
    type_of_processing: str = Field(..., min_length=1)
    net_weight: float = Field(0, gt=0)
    waste_from_cold_processing: float = Field(0, gt=0)
    waste_from_heat_processing: float = Field(0, gt=0)
    quantity: float = Field(0, gt=0)
    gross_weight: float = Field(0, gt=0)
    output: float = Field(0, gt=0)


class TechCardCreate(TechCardBase):
    items: List[TechCardItemCreate]


class TechCardItem(TechCardItemCreate):
    id: UUID
    tech_card_id: UUID

    class Config:
        orm_mode = True


class TechCardResponse(TechCard):
    items: List[TechCardItem]

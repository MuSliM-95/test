from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional, List
from datetime import datetime
from uuid import UUID


class TechCardStatus(str, Enum):
    active = "active"
    canceled = "canceled"
    deleted = "deleted"


class TechOperationComponentQuantities(BaseModel):
    name: str = Field(..., min_length=1)
    nomenclature_id: int
    quantity: float


class TechOperationBase(BaseModel):
    tech_card_id: UUID
    output_quantity: float
    from_warehouse_id: int
    to_warehouse_id: int
    nomenclature_id: int
    # количество выпускаемого изделия
    component_quantities: List[TechOperationComponentQuantities]
    payment_ids: Optional[List[UUID]] = None


class TechOperationCreate(TechOperationBase):
    pass


class TechOperation(TechOperationBase):
    id: UUID
    user_id: int
    created_at: datetime
    status: TechCardStatus
    production_order_id: UUID
    consumption_order_id: UUID

    class Config:
        orm_mode = True


class TechOperationComponentCreate(BaseModel):
    # component_id: UUID
    name: str = Field(..., min_length=1)
    quantity: float
    nomeclature_id: int
    gross_weight: Optional[float] = None
    net_weight: Optional[float] = None

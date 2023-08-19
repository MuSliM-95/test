from pydantic import BaseModel
from typing import Optional, List


class View(BaseModel):
    id: int
    organization_id: Optional[int]
    warehouse_id: Optional[int]
    nomenclature_id: Optional[int]
    incoming_amount: Optional[int]
    outgoing_amount: Optional[int]
    current_amount: int
    cashbox_id: Optional[int]
    updated_at: int
    created_at: int


class ListView(BaseModel):
    __root__: Optional[List[View]]

    class Config:
        orm_mode = True

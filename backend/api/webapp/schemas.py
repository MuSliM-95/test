from pydantic import BaseModel
from typing import Optional, List
from api.pictures.schemas import Picture
from api.price_types.schemas import PriceType
from api.warehouses.schemas import Warehouse


class PriceInList(BaseModel):
    id: int
    type: Optional[str]
    code: Optional[int]
    unit: Optional[int]
    unit_name: Optional[str]
    category: Optional[int]
    category_name: Optional[str]
    manufacturer: Optional[int]
    manufacturer_name: Optional[str]
    price: float
    date_from: Optional[int]
    date_to: Optional[int]
    price_types: Optional[List[PriceType]]

    class Config:
        orm_mode = True

class ViewAlt(BaseModel):
    id: int
    name: str
    category: Optional[int]
    organization_id: Optional[int]
    organization_name: Optional[str]
    warehouse_id: Optional[int]
    warehouse_name: Optional[str]
    current_amount: float
    plus_amount: float
    minus_amount: float
    start_ost: float
    now_ost: float
    warehouses: Optional[List[Warehouse]]


class ViewAltList(BaseModel):
    name: str
    key: int
    children: List[ViewAlt]


class WebappItem(BaseModel):
    id: int
    name: str
    type: Optional[str]
    description_short: Optional[str]
    description_long: Optional[str]
    code: Optional[int]
    unit: Optional[int]
    category: Optional[int]
    manufacturer: Optional[int]
    updated_at: int
    created_at: int
    unit_name: Optional[str]
    pictures: Optional[List[Picture]]
    prices: Optional[List[PriceInList]]
    alt_warehouse_balances: Optional[List[ViewAltList]]



class WebappResponse(BaseModel):
    result: Optional[List[WebappItem]]
    count: int


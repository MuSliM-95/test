from pydantic import BaseModel
from typing import Optional, List
from api.pictures.schemas import Picture
from api.price_types.schemas import PriceType
from api.warehouse_balances.schemas import ViewAltList
from api.warehouses.schemas import Warehouse


class PriceInList(BaseModel):
    id: int
    nomenclature_id: int
    nomenclature_name: str
    type: Optional[str]
    description_short: Optional[str]
    description_long: Optional[str]
    code: Optional[int]
    unit: Optional[int]
    unit_name: Optional[str]
    category: Optional[int]
    category_name: Optional[str]
    manufacturer: Optional[int]
    manufacturer_name: Optional[str]
    price: float
    price_type: Optional[str]
    date_from: Optional[int]
    date_to: Optional[int]
    price_types: Optional[List[PriceType]]

    class Config:
        orm_mode = True

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
    warehouses: Optional[List[Warehouse]]


class WebappResponse(BaseModel):
    result: Optional[List[WebappItem]]
    count: int


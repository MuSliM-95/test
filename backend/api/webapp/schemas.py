from pydantic import BaseModel
from typing import Optional, List
from api.pictures.schemas import Picture
from api.price_types.schemas import PriceType
from api.prices.schemas import PriceInList
from api.warehouse_balances.schemas import ViewAltList


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
    # pictures: Optional[List[Picture]]
    # price_types: Optional[List[PriceType]]
    # prices: Optional[List[PriceInList]]
    # alt_warehouse_balances: Optional[List[ViewAltList]]


class WebappResponse(BaseModel):
    result: Optional[List[WebappItem]]
    count: int


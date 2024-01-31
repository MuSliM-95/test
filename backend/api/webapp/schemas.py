from pydantic import BaseModel
from typing import Optional, List

class Item(BaseModel):
    id: int
    entity: str
    entity_id: int
    is_main: Optional[bool]
    url: str
    size: Optional[int]
    updated_at: int
    created_at: int




class NomenclatureCreate(BaseModel):
    name: str
    type: Optional[str]
    description_short: Optional[str]
    description_long: Optional[str]
    code: Optional[int]
    unit: Optional[int]
    category: Optional[int]
    manufacturer: Optional[int]
    pictures: Optional[List[Item]]


class NomenclatureListGetRes(BaseModel):
    result: Optional[List[NomenclatureCreate]]
    count: int
    id_massive Optional[List[str]]

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class WarehouseCreate(BaseModel):
    name: str
    type: Optional[str] = None
    description: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    parent: Optional[int] = None
    is_public: Optional[bool] = None

    status: bool = True

    shop_schedule: Optional[Dict[str, Any]] = None
    delivery_schedule: Optional[Dict[str, Any]] = None

    class Config:
        orm_mode = True


class WarehouseCreateMass(BaseModel):
    __root__: List[WarehouseCreate]

    class Config:
        orm_mode = True


class WarehouseUpdate(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    parent: Optional[int] = None
    is_public: Optional[bool] = None
    status: Optional[bool] = None

    shop_schedule: Optional[Dict[str, Any]] = None
    delivery_schedule: Optional[Dict[str, Any]] = None

    class Config:
        orm_mode = True


class WarehouseEdit(WarehouseUpdate):
    pass


class Warehouse(WarehouseCreate):
    id: int
    latitude: Optional[float]
    longitude: Optional[float]
    created_at: int
    updated_at: int

    class Config:
        orm_mode = True


class WarehouseList(BaseModel):
    __root__: List[Warehouse]

    class Config:
        orm_mode = True


class WarehouseListGet(BaseModel):
    result: List[Warehouse]
    count: int

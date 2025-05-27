from pydantic import BaseModel
from typing import Optional, List

from api.prices.schemas import PriceInList, PriceGetWithNomenclature
from api.warehouse_balances.schemas import WarehouseWithNomenclature
from database.db import NomenclatureCashbackType


class NomenclatureBarcodeCreate(BaseModel):
    barcode: str


class NomenclatureCreate(BaseModel):
    name: str
    type: Optional[str]
    description_short: Optional[str]
    description_long: Optional[str]
    code: Optional[str]
    unit: Optional[int]
    category: Optional[int]
    manufacturer: Optional[int]
    cashback_type: Optional[NomenclatureCashbackType] = NomenclatureCashbackType.no_cashback
    cashback_value: Optional[int] = 0
    external_id: Optional[str]

    class Config:
        orm_mode = True


class NomenclatureCreateMass(BaseModel):
    __root__: List[NomenclatureCreate]

    class Config:
        orm_mode = True


class NomenclatureEdit(NomenclatureCreate):
    name: Optional[str]


class NomenclatureEditMass(NomenclatureEdit):
    id: int


class Nomenclature(NomenclatureCreate):
    id: int
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class NomenclatureGet(NomenclatureCreate):
    id: int
    unit_name: Optional[str]
    barcodes: Optional[List[str]]
    prices: Optional[List[PriceGetWithNomenclature]]
    balances: Optional[List[WarehouseWithNomenclature]]
    group_id: Optional[int]
    group_name: Optional[str]
    is_main: Optional[bool]
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class NomenclatureList(BaseModel):
    __root__: Optional[List[Nomenclature]]

    class Config:
        orm_mode = True


class NomenclatureListGet(BaseModel):
    __root__: Optional[List[NomenclatureGet]]

    class Config:
        orm_mode = True


class NomenclatureListGetRes(BaseModel):
    result: Optional[List[NomenclatureGet]]
    count: int


class NomenclaturesListPatch(BaseModel):
    idx: int
    old_barcode: Optional[str]
    new_barcode: str

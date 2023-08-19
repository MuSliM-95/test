from pydantic import BaseModel
from typing import Optional, List


class ManufacturerCreate(BaseModel):
    name: str

    class Config:
        orm_mode = True


class ManufacturerCreateMass(BaseModel):
    __root__: List[ManufacturerCreate]

    class Config:
        orm_mode = True


class ManufacturerEdit(ManufacturerCreate):
    name: Optional[str]


class Manufacturer(ManufacturerCreate):
    id: int
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class ManufacturerList(BaseModel):
    __root__: Optional[List[Manufacturer]]

    class Config:
        orm_mode = True

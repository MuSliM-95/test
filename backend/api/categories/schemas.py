from pydantic import BaseModel
from typing import Optional, List


class CategoryCreate(BaseModel):
    name: str
    description: Optional[str]
    code: Optional[int]
    parent: Optional[int]
    status: bool = True

    class Config:
        orm_mode = True


class CategoryCreateMass(BaseModel):
    __root__: List[CategoryCreate]

    class Config:
        orm_mode = True


class CategoryEdit(CategoryCreate):
    name: Optional[str]
    status: Optional[bool]


class Category(CategoryCreate):
    id: int
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class CategoryList(BaseModel):
    __root__: Optional[List[Category]]

    class Config:
        orm_mode = True

from pydantic import BaseModel
from typing import Optional, List, Dict


class Tag(BaseModel):
    id: int
    name: str

    class Config:
        orm_mode = True


class TypeTemplate(BaseModel):
    id: int
    name: str

    class Config:
        orm_mode = True


class DocTemplateCreate(BaseModel):
    name: str
    description: Optional[str] = None
    template_data: Optional[str] = None
    tags: Optional[str] = None
    user_id: int
    is_deleted: bool = False
    type: int = None

    class Config:
        orm_mode = True


class DocTemplateUpdate(BaseModel):
    id: Optional[int]
    name: Optional[str]
    description: Optional[str] = None
    template_data: Optional[str] = None
    tags: Optional[str] = None
    user_id: Optional[int]
    is_deleted: Optional[bool] = False
    type: Optional[int] = None

    class Config:
        orm_mode = True


class DocTemplate(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    template_data: str = None
    tags: Optional[str] = None
    user_id: str
    created_at: int
    updated_at: int
    is_deleted: Optional[bool]
    type: int = None

    class Config:
        orm_mode = True


class TemplateList(BaseModel):
    result: Optional[List[DocTemplate]]
    tags: Optional[str] = None


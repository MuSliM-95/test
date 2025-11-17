"""Pydantic схемы для категорий"""
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class GlobalCategoryBase(BaseModel):
    """Базовая схема категории"""

    model_config = ConfigDict(from_attributes=True)

    name: str
    description: Optional[str] = None
    code: Optional[int] = None
    parent_id: Optional[int] = None
    external_id: Optional[str] = None
    image_url: Optional[str] = None
    is_active: bool = True


class GlobalCategoryCreate(GlobalCategoryBase):
    """Схема для создания категории"""

    pass


class GlobalCategoryUpdate(BaseModel):
    """Схема для обновления категории - все поля опциональны"""

    model_config = ConfigDict(from_attributes=True)

    name: Optional[str] = None
    description: Optional[str] = None
    code: Optional[int] = None
    parent_id: Optional[int] = None
    external_id: Optional[str] = None
    image_url: Optional[str] = None
    is_active: Optional[bool] = None


class GlobalCategory(GlobalCategoryBase):
    """Категория с ID и временными метками"""
    id: int
    created_at: str
    updated_at: str


class GlobalCategoryTree(GlobalCategory):
    """Категория с дочерними элементами"""
    children: Optional[List['GlobalCategoryTree']] = []


class GlobalCategoryList(BaseModel):
    """Список категорий с количеством"""
    result: List[GlobalCategory]
    count: int


class GlobalCategoryTreeList(BaseModel):
    """Дерево категорий с количеством"""
    result: List[GlobalCategoryTree]
    count: int

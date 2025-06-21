import re

from pydantic import BaseModel, validator
from typing import List, Optional


class CBUsers(BaseModel):
    id: int
    phone_number: Optional[str]
    external_id: Optional[str]
    first_name: str
    last_name: Optional[str]
    username: Optional[str]
    status: bool
    photo: str
    is_admin: bool
    created_at: int
    updated_at: int
    tags: Optional[str]

    class Config:
        orm_mode = True


class CBUsersShort(BaseModel):
    id: int
    external_id: Optional[str]
    first_name: str
    last_name: Optional[str]
    status: bool

    class Config:
        orm_mode = True


class CBUsersListShort(BaseModel):
    result: List[CBUsersShort]
    count: int

    class Config:
        orm_mode = True


class CBUsersList(BaseModel):
    result: List[CBUsers]
    count: int

    class Config:
        orm_mode = True


class PermissionItem(BaseModel):
    section: str
    can_view: bool = True
    can_edit: bool = False
    paybox_id: Optional[int] = None


class UserPermissionUpdate(BaseModel):
    user_id: int
    permissions: List[PermissionItem]


class UserPermissionResponse(BaseModel):
    status: str = "success"
    message: str = "Права пользователя обновлены"


class UserPermissionsList(BaseModel):
    section: str
    can_view: bool
    can_edit: bool
    paybox_id: Optional[int] = None
    paybox_name: Optional[str] = None


class UserPermissionsResult(BaseModel):
    is_admin: Optional[bool] = None
    user_id: int
    first_name: str
    last_name: Optional[str]
    username: Optional[str]
    permissions: List[UserPermissionsList]


class UserTagsUpdate(BaseModel):
    tags: Optional[str]

    @validator("tags")
    def validate_tags(cls, v):
        tag_list = [tag.strip() for tag in v.split(",") if tag.strip()]

        if len(tag_list) > 10:
            raise ValueError("Максимум 10 тегов")

        if len(set(tag_list)) < len(tag_list):
            raise ValueError("Теги не должны повторяться")

        pattern = re.compile(r"^[a-zA-Zа-яА-Я0-9_-]{2,20}$")
        for tag in tag_list:
            if not pattern.match(tag):
                raise ValueError(
                    f"Тег '{tag}' содержит недопустимые символы или некорректную длину (2–20 символов)"
                )

        return v

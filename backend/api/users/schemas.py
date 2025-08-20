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
    tags: Optional[List[str]]
    timezone: Optional[str]
    payment_past_edit_days: Optional[int]

    class Config:
        orm_mode = True

    @validator("tags")
    def validate_tags(cls, tag_list):
        return tag_list or []


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

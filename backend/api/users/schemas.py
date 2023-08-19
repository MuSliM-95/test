from pydantic import BaseModel
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

    class Config:
        orm_mode = True


class CBUsersList(BaseModel):
    result: List[CBUsers]
    count: int

    class Config:
        orm_mode = True

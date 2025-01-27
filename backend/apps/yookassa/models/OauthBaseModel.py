from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class OauthBaseModel(BaseModel):
    cashbox_id: Optional[int] = None
    access_token: str
    is_deleted: bool = False


class OauthModel(OauthBaseModel):
    id: int
    created_at: datetime
    updated_at: datetime


class OauthUpdateModel(OauthBaseModel):
    cashbox_id: Optional[int] = None
    access_token: Optional[str] = None


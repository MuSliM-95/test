from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class OauthBaseModel(BaseModel):
    cashbox_id: Optional[int] = None
    access_token: str = None
    is_deleted: bool = False
    warehouse_id: Optional[int] = None


class OauthModel(OauthBaseModel):
    id: int
    created_at: datetime
    updated_at: datetime


class OauthUpdateModel(OauthBaseModel):
    cashbox_id: Optional[int] = None
    access_token: Optional[str] = None
    warehouse_id: Optional[int] = None


class OauthWarehouseModel(BaseModel):
    warehouse_name: str = None
    warehouse_description: str = None
    last_update: datetime = None
    status: bool = None



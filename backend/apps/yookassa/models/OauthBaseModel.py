from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class OauthBaseModel(BaseModel):
    id: int = None
    cashbox_id: Optional[int] = None
    access_token: str = None
    is_deleted: bool = False
    warehouse_id: Optional[int] = None


class OauthModel(OauthBaseModel):
    created_at: datetime
    updated_at: datetime


class OauthUpdateModel(OauthBaseModel):
    cashbox_id: Optional[int] = None
    access_token: Optional[str] = None
    warehouse_id: Optional[int] = None
    is_deleted: bool = None


class OauthWarehouseModel(BaseModel):
    warehouse_name: str = None
    warehouse_description: str = None
    last_update: datetime = None
    warehouse_id: int = None
    status: bool = None



from pydantic import BaseModel, Field
from typing import Optional


class EvotorInstallEvent(BaseModel):
    id: Optional[str] = None
    timestamp: Optional[int] = None
    version: Optional[float] = None
    type: Optional[str] = None
    data: Optional[dict] = None


class EvotorHeader(BaseModel):
    user_agent: str = None
    authorization: str = None


class EvotorUserToken(BaseModel):
    userId: str
    evotor_token: str = Field(alias = "token")

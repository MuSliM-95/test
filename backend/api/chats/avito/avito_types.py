from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime


class AvitoImageSize(BaseModel):
    """Image size variant"""
    url: str


class AvitoImage(BaseModel):
    """Image content"""
    sizes: Dict[str, str] 


class AvitoItemContent(BaseModel):
    """Item (product) content"""
    id: int
    title: str
    user_id: int
    status_id: int
    price_string: str
    url: str
    images: Optional[Dict[str, Any]] = None


class AvitoLinkContent(BaseModel):
    """Link content"""
    text: str
    url: str


class AvitoLocationContent(BaseModel):
    """Location content"""
    text: str
    title: str
    kind: str
    lat: float
    lon: float


class AvitoVoiceContent(BaseModel):
    """Voice message content"""
    duration: Optional[int] = None
    url: Optional[str] = None
    voice_id: Optional[str] = None
    voice_url: Optional[str] = None


class AvitoFileContent(BaseModel):
    """File content"""
    url: str
    name: str


class AvitoMessageContent(BaseModel):
    """Message content - can be text, image, item, link, location, voice, file, etc."""
    text: Optional[str] = None
    image: Optional[AvitoImage] = None
    item: Optional[AvitoItemContent] = None
    link: Optional[AvitoLinkContent] = None
    location: Optional[AvitoLocationContent] = None
    voice: Optional[AvitoVoiceContent] = None
    file: Optional[AvitoFileContent] = None


class AvitoMessage(BaseModel):
    """Message from Avito API"""
    id: str
    chat_id: str
    user_id: int
    author_id: int
    created: int 
    type: str 
    chat_type: str 
    content: AvitoMessageContent


class AvitoWebhookValue(BaseModel):
    id: Optional[str] = None
    chat_id: Optional[str] = None
    user_id: Optional[int] = None
    author_id: Optional[int] = None
    created: Optional[int] = None
    published_at: Optional[str] = None
    type: Optional[str] = None
    chat_type: Optional[str] = None
    item_id: Optional[int] = None
    content: Optional[Dict[str, Any]] = None
    read: Optional[int] = None


class AvitoWebhookPayload(BaseModel):
    """Webhook payload"""
    type: str 
    value: AvitoWebhookValue


class AvitoWebhook(BaseModel):
    """Complete webhook from Avito"""
    id: str
    version: str
    timestamp: int
    payload: AvitoWebhookPayload


class AvitoWebhookRequest(BaseModel):
    """Incoming webhook request"""
    webhook: AvitoWebhook


class AvitoCredentials(BaseModel):
    """Avito API credentials"""
    api_key: str
    client_id: str
    client_secret: str
    access_token: Optional[str] = None


class AvitoConnectRequest(BaseModel):
    """Request to connect Avito to cashbox"""
    channel_id: int
    credentials: AvitoCredentials


class AvitoChannelResponse(BaseModel):
    """Response for connected Avito channel"""
    channel_id: int
    cashbox_id: int
    is_active: bool
    created_at: datetime
    message: str


class AvitoWebhookResponse(BaseModel):
    """Response for webhook processing"""
    success: bool
    message: str
    chat_id: Optional[int] = None
    message_id: Optional[int] = None

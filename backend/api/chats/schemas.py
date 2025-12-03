from typing import Optional, List, Dict, Any
from pydantic import BaseModel, validator
from datetime import datetime



class ChannelCreate(BaseModel):
    name: str
    type: str
    description: Optional[str] = None
    svg_icon: Optional[str] = None
    tags: Optional[dict] = None
    api_config_name: Optional[str] = None


class ChannelUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    svg_icon: Optional[str] = None
    tags: Optional[dict] = None
    api_config_name: Optional[str] = None
    is_active: Optional[bool] = None


class ChannelResponse(BaseModel):
    id: int
    name: str
    type: str
    description: Optional[str]
    svg_icon: Optional[str]
    tags: Optional[dict]
    api_config_name: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime

class ChatCreate(BaseModel):
    channel_id: int
    external_chat_id: str
    phone: Optional[str] = None
    name: Optional[str] = None
    # contragent_id теперь привязывается через chat_contact
    assigned_operator_id: Optional[int] = None


class ChatUpdate(BaseModel):
    status: Optional[str] = None
    assigned_operator_id: Optional[int] = None
    # phone и name теперь обновляются через chat_contact
    phone: Optional[str] = None
    name: Optional[str] = None
    first_message_time: Optional[datetime] = None
    first_response_time_seconds: Optional[int] = None
    last_message_time: Optional[datetime] = None
    last_response_time_seconds: Optional[int] = None

    @validator('first_message_time', 'last_message_time', pre=True)
    def convert_datetime(cls, v):
        if v is None:
            return None
        if isinstance(v, datetime):
            if v.tzinfo is not None:
                return v.replace(tzinfo=None)
            return v
        return v


class ContactInfo(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    avatar: Optional[str] = None
    contragent_id: Optional[int] = None


class ChatResponse(BaseModel):
    id: int
    channel_id: int
    chat_contact_id: Optional[int] = None
    cashbox_id: int
    external_chat_id: str
    status: str
    assigned_operator_id: Optional[int] = None
    first_message_time: Optional[datetime] = None
    first_response_time_seconds: Optional[int] = None
    last_message_time: Optional[datetime] = None
    last_response_time_seconds: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    last_message_preview: Optional[str] = None
    unread_count: int = 0
    channel_name: Optional[str] = None
    channel_icon: Optional[str] = None
    channel_type: Optional[str] = None
    name: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    contact: Optional[ContactInfo] = None



class MessageCreate(BaseModel):
    chat_id: int
    sender_type: str
    content: str
    message_type: str = "TEXT"
    status: str = "SENT"
    image_url: Optional[str] = None
    source: Optional[str] = None


class MessageUpdate(BaseModel):
    status: Optional[str] = None
    content: Optional[str] = None


class MessageResponse(BaseModel):
    id: int
    chat_id: int
    sender_type: str
    message_type: str
    content: str
    external_message_id: Optional[str]
    status: str
    created_at: datetime
    updated_at: datetime
    sender_avatar: Optional[str] = None
    source: Optional[str] = None

    @validator('created_at', 'updated_at', pre=True)
    def convert_datetime(cls, v):
        if v is None:
            return None
        if isinstance(v, datetime):
            if v.tzinfo is not None:
                return v.replace(tzinfo=None)
            return v
        return v


class ChainClientRequest(BaseModel):
    phone: str
    name: Optional[str] = None


class ChainClientResponse(BaseModel):
    chat: ChatResponse
    contragent_id: Optional[int]
    contragent_name: Optional[str]
    is_new_contragent: bool
    message: str


class ChannelsList(BaseModel):
    data: List[ChannelResponse]
    total: int
    skip: int
    limit: int


class ChatsList(BaseModel):
    data: List[ChatResponse]
    total: int
    skip: int
    limit: int


class MessagesList(BaseModel):
    data: List[MessageResponse]
    total: int
    skip: int
    limit: int
    date: Optional[datetime] = None

    @validator('date', pre=True)
    def convert_datetime(cls, v):
        if v is None:
            return None
        if isinstance(v, datetime):
            if v.tzinfo is not None:
                return v.replace(tzinfo=None)
            return v
        return v


class ManagerInChat(BaseModel):
    """Информация о менеджере, подключенном к чату"""
    user_id: int
    user_type: str
    connected_at: str


class ManagersInChatResponse(BaseModel):
    """Ответ со списком менеджеров в чате"""
    chat_id: int
    managers: List[ManagerInChat]
    total: int

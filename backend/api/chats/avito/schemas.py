from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime


class AvitoCredentialsCreate(BaseModel):
    api_key: str
    api_secret: str
    class Config:
        json_schema_extra = {
            "example": {
                "api_key": "your_client_id",
                "api_secret": "your_client_secret"
            }
        }


class AvitoCredentialsResponse(BaseModel):
    id: int
    channel_id: int
    cashbox_id: int
    created_at: datetime
    
    class Config:
        from_attributes = True


class AvitoUser(BaseModel):
    user_id: str
    name: Optional[str] = None
    phone: Optional[str] = None
    rating: Optional[float] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "123456",
                "name": "Иван Петров",
                "phone": "+79991234567",
                "rating": 4.5
            }
        }


class AvitoMessage(BaseModel):
    message_id: str
    chat_id: str
    user: AvitoUser
    text: str
    created_at: str
    attachments: Optional[list] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "message_id": "msg_123456",
                "chat_id": "chat_789",
                "user": {
                    "user_id": "123456",
                    "name": "Иван Петров",
                    "phone": "+79991234567"
                },
                "text": "Привет, есть товар в наличии?",
                "created_at": "2025-11-12T10:30:00Z",
                "attachments": []
            }
        }


class AvitoWebhookEvent(BaseModel):
    event_type: str  
    data: Dict[str, Any]
    timestamp: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "message_received",
                "data": {
                    "message_id": "msg_123456",
                    "chat_id": "chat_789"
                },
                "timestamp": "2025-11-12T10:30:00Z"
            }
        }


class AvitoWebhookResponse(BaseModel):
    success: bool
    message: str
    chat_id: Optional[int] = None
    message_id: Optional[int] = None


class AvitoSyncResponse(BaseModel):
    synced_count: int
    new_messages: int
    updated_messages: int
    errors: Optional[list] = None


class AvitoWebhookRegisterRequest(BaseModel):
    webhook_url: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "webhook_url": "https://your-domain.com/chats/avito/webhooks/message"
            }
        }


class AvitoWebhookRegisterResponse(BaseModel):
    success: bool
    message: str
    webhook_url: Optional[str] = None
    webhook_id: Optional[str] = None


class AvitoWebhookUpdateResponse(BaseModel):
    success: bool
    message: str
    updated_channels: int
    failed_channels: int
    results: Optional[list] = None


class AvitoConnectResponse(BaseModel):
    success: bool
    message: str
    channel_id: int
    cashbox_id: int
    webhook_registered: Optional[bool] = None
    webhook_url: Optional[str] = None
    webhook_error: Optional[str] = None


class AvitoChatListItem(BaseModel):
    id: str
    created: Optional[int] = None
    updated: Optional[int] = None
    last_message: Optional[Dict[str, Any]] = None
    users: Optional[List[Dict[str, Any]]] = None
    context: Optional[Dict[str, Any]] = None


class AvitoChatsListResponse(BaseModel):
    success: bool
    total: int
    chats: List[AvitoChatListItem]
    created_in_db: int
    updated_in_db: int


class AvitoMessageItem(BaseModel):
    id: str
    author_id: Optional[int] = None
    created: Optional[int] = None
    content: Optional[Dict[str, Any]] = None
    type: Optional[str] = None
    direction: Optional[str] = None
    is_read: Optional[bool] = None
    read: Optional[int] = None


class AvitoMessagesResponse(BaseModel):
    success: bool
    chat_id: int
    external_chat_id: str
    total: int
    messages: List[AvitoMessageItem]
    saved_to_db: int


class AvitoHistoryLoadResponse(BaseModel):
    success: bool
    channel_id: int
    from_date: int
    chats_processed: int
    chats_created: int
    chats_updated: int
    messages_loaded: int
    messages_created: int
    messages_updated: int
    errors: Optional[List[str]] = None


class AvitoApplicantPhoneResponse(BaseModel):
    success: bool
    chat_id: int
    phone: Optional[str] = None
    name: Optional[str] = None


class AvitoChatMetadataResponse(BaseModel):
    success: bool
    chat_id: int
    external_chat_id: str
    metadata: Dict[str, Any]


class AvitoOAuthAuthorizeResponse(BaseModel):
    """Ответ с URL для OAuth авторизации"""
    authorization_url: str
    state: Optional[str] = None
    message: str = "Перейдите по ссылке для авторизации в Avito"


class AvitoOAuthCallbackResponse(BaseModel):
    """Ответ после успешного OAuth callback"""
    success: bool
    message: str
    channel_id: Optional[int] = None
    cashbox_id: Optional[int] = None
    webhook_registered: Optional[bool] = None
    webhook_url: Optional[str] = None
    webhook_error: Optional[str] = None

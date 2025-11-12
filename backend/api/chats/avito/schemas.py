from pydantic import BaseModel
from typing import Optional, Dict, Any
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


class AvitoSendMessageRequest(BaseModel):
    chat_id: int  
    content: str
    message_type: str = "TEXT"
    
    class Config:
        json_schema_extra = {
            "example": {
                "chat_id": 10,
                "content": "Спасибо за покупку!",
                "message_type": "TEXT"
            }
        }


class AvitoSendMessageResponse(BaseModel):
    success: bool
    message_id: Optional[str] = None
    external_message_id: Optional[str] = None


class AvitoConnectResponse(BaseModel):
    success: bool
    message: str
    channel_id: int
    cashbox_id: int

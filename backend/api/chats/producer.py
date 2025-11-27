import uuid
from uuid import UUID
from typing import Optional
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage
from common.utils.ioc.ioc import ioc
from datetime import datetime


class ChatMessageModel(BaseModelMessage):
    chat_id: int
    message_id_value: int
    sender_type: str
    content: str
    message_type: str
    timestamp: str


class ChatTypingEventModel(BaseModelMessage):
    chat_id: int
    user_id: int
    user_type: str
    is_typing: bool
    timestamp: str


class ChatUserConnectedEventModel(BaseModelMessage):
    chat_id: int
    user_id: int
    user_type: str
    timestamp: str


class ChatUserDisconnectedEventModel(BaseModelMessage):
    chat_id: int
    user_id: int
    user_type: str
    timestamp: str


class ChatMessageProducer:
    """Producer для отправки сообщений чатов в RabbitMQ"""
    
    async def send_message(self, chat_id: int, message_data: dict):
        """Отправить сообщение в очередь"""
        try:
            rabbit_messaging: IRabbitMessaging = await ioc.get(IRabbitFactory)()
            
            message = ChatMessageModel(
                message_id=uuid.uuid4(),
                chat_id=chat_id,
                message_id_value=message_data.get("message_id"),
                sender_type=message_data.get("sender_type", "OPERATOR"),
                content=message_data.get("content", ""),
                message_type=message_data.get("message_type", "TEXT"),
                timestamp=message_data.get("timestamp") or datetime.utcnow().isoformat()
            )
            
            await rabbit_messaging.publish(
                message=message,
                routing_key="chat.messages"
            )
            
            print(f"[PRODUCER] Message sent to RabbitMQ for chat {chat_id}: {message_data.get('message_id')}")
            
        except Exception as e:
            print(f"[PRODUCER] Failed to send message to RabbitMQ: {e}")
            import traceback
            traceback.print_exc()
    
    async def send_typing_event(self, chat_id: int, user_id: int, user_type: str, is_typing: bool):
        """Отправить событие печати в очередь"""
        try:
            rabbit_messaging: IRabbitMessaging = await ioc.get(IRabbitFactory)()
            
            event = ChatTypingEventModel(
                message_id=uuid.uuid4(),
                chat_id=chat_id,
                user_id=user_id,
                user_type=user_type,
                is_typing=is_typing,
                timestamp=datetime.utcnow().isoformat()
            )
            
            await rabbit_messaging.publish(
                message=event,
                routing_key="chat.events.typing"
            )
            
            print(f"[PRODUCER] Typing event sent to RabbitMQ for chat {chat_id}, user {user_id}, typing: {is_typing}")
            
        except Exception as e:
            print(f"[PRODUCER] Failed to send typing event to RabbitMQ: {e}")
            import traceback
            traceback.print_exc()
    
    async def send_user_connected_event(self, chat_id: int, user_id: int, user_type: str):
        """Отправить событие подключения пользователя в очередь"""
        try:
            rabbit_messaging: IRabbitMessaging = await ioc.get(IRabbitFactory)()
            
            event = ChatUserConnectedEventModel(
                message_id=uuid.uuid4(),
                chat_id=chat_id,
                user_id=user_id,
                user_type=user_type,
                timestamp=datetime.utcnow().isoformat()
            )
            
            await rabbit_messaging.publish(
                message=event,
                routing_key="chat.events.user_connected"
            )
            
            print(f"[PRODUCER] User connected event sent to RabbitMQ for chat {chat_id}, user {user_id}")
            
        except Exception as e:
            print(f"[PRODUCER] Failed to send user connected event to RabbitMQ: {e}")
            import traceback
            traceback.print_exc()
    
    async def send_user_disconnected_event(self, chat_id: int, user_id: int, user_type: str):
        """Отправить событие отключения пользователя в очередь"""
        try:
            rabbit_messaging: IRabbitMessaging = await ioc.get(IRabbitFactory)()
            
            event = ChatUserDisconnectedEventModel(
                message_id=uuid.uuid4(),
                chat_id=chat_id,
                user_id=user_id,
                user_type=user_type,
                timestamp=datetime.utcnow().isoformat()
            )
            
            await rabbit_messaging.publish(
                message=event,
                routing_key="chat.events.user_disconnected"
            )
            
            print(f"[PRODUCER] User disconnected event sent to RabbitMQ for chat {chat_id}, user {user_id}")
            
        except Exception as e:
            print(f"[PRODUCER] Failed to send user disconnected event to RabbitMQ: {e}")
            import traceback
            traceback.print_exc()

chat_producer = ChatMessageProducer()
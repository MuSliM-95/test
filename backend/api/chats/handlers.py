from typing import Mapping, Any, Optional
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from api.chats.producer import (
    ChatMessageModel,
    ChatTypingEventModel,
    ChatUserConnectedEventModel,
    ChatUserDisconnectedEventModel
)
from api.chats.websocket import chat_manager
from aio_pika import IncomingMessage


class ChatMessageHandler(IEventHandler):
    """Обработчик сообщений чатов из RabbitMQ"""
    
    async def __call__(self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None):
        """Обработать сообщение чата из RabbitMQ"""
        try:
            chat_message = ChatMessageModel(**event)
            
            chat_id = chat_message.chat_id
            
            print(f"[CONSUMER] Message received from RabbitMQ for chat {chat_id}: {chat_message.message_id_value}")
            
            ws_message = {
                "type": "message",
                "message_id": chat_message.message_id_value,
                "chat_id": chat_id,
                "sender_type": chat_message.sender_type,
                "content": chat_message.content,
                "message_type": chat_message.message_type,
                "status": "delivered",
                "timestamp": chat_message.timestamp
            }
            
            await chat_manager.broadcast_to_chat(chat_id, ws_message)
            
            print(f"[CONSUMER] Message broadcasted to chat {chat_id}")
            
        except Exception as e:
            print(f"[CONSUMER] Error processing message from RabbitMQ: {e}")
            import traceback
            traceback.print_exc()
            # Пробрасываем исключение для правильной обработки RabbitMQ (повтор или удаление из очереди)
            raise


class ChatTypingEventHandler(IEventHandler):
    """Обработчик событий печати из RabbitMQ"""
    
    async def __call__(self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None):
        """Обработать событие печати из RabbitMQ"""
        try:
            typing_event = ChatTypingEventModel(**event)
            
            chat_id = typing_event.chat_id
            
            print(f"[CONSUMER] Typing event received from RabbitMQ for chat {chat_id}, user {typing_event.user_id}, typing: {typing_event.is_typing}")
            
            ws_message = {
                "type": "typing",
                "chat_id": chat_id,
                "user_id": typing_event.user_id,
                "user_type": typing_event.user_type,
                "is_typing": typing_event.is_typing,
                "timestamp": typing_event.timestamp
            }
            
            await chat_manager.broadcast_to_chat(chat_id, ws_message)
            
            print(f"[CONSUMER] Typing event broadcasted to chat {chat_id}")
            
        except Exception as e:
            print(f"[CONSUMER] Error processing typing event from RabbitMQ: {e}")
            import traceback
            traceback.print_exc()
            raise


class ChatUserConnectedEventHandler(IEventHandler):
    """Обработчик событий подключения пользователя из RabbitMQ"""
    
    async def __call__(self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None):
        """Обработать событие подключения пользователя из RabbitMQ"""
        try:
            connect_event = ChatUserConnectedEventModel(**event)
            
            chat_id = connect_event.chat_id
            
            print(f"[CONSUMER] User connected event received from RabbitMQ for chat {chat_id}, user {connect_event.user_id}")
            
            ws_message = {
                "type": "user_connected",
                "chat_id": chat_id,
                "user_id": connect_event.user_id,
                "user_type": connect_event.user_type,
                "timestamp": connect_event.timestamp
            }
            
            await chat_manager.broadcast_to_chat(chat_id, ws_message)
            
            print(f"[CONSUMER] User connected event broadcasted to chat {chat_id}")
            
        except Exception as e:
            print(f"[CONSUMER] Error processing user connected event from RabbitMQ: {e}")
            import traceback
            traceback.print_exc()
            raise


class ChatUserDisconnectedEventHandler(IEventHandler):
    """Обработчик событий отключения пользователя из RabbitMQ"""
    
    async def __call__(self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None):
        """Обработать событие отключения пользователя из RabbitMQ"""
        try:
            disconnect_event = ChatUserDisconnectedEventModel(**event)
            
            chat_id = disconnect_event.chat_id
            
            print(f"[CONSUMER] User disconnected event received from RabbitMQ for chat {chat_id}, user {disconnect_event.user_id}")
            
            ws_message = {
                "type": "user_disconnected",
                "chat_id": chat_id,
                "user_id": disconnect_event.user_id,
                "user_type": disconnect_event.user_type,
                "timestamp": disconnect_event.timestamp
            }
            
            await chat_manager.broadcast_to_chat(chat_id, ws_message)
            
            print(f"[CONSUMER] User disconnected event broadcasted to chat {chat_id}")
            
        except Exception as e:
            print(f"[CONSUMER] Error processing user disconnected event from RabbitMQ: {e}")
            import traceback
            traceback.print_exc()
            raise

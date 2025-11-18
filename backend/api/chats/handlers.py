import json
from typing import Mapping, Any, Optional
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from api.chats.producer import ChatMessageModel
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

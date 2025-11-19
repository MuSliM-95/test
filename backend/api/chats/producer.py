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

chat_producer = ChatMessageProducer()
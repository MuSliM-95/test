import asyncio
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.amqp_messaging.common.impl.models.QueueSettingsModel import QueueSettingsModel
from common.utils.ioc.ioc import ioc
from api.chats.producer import (
    ChatMessageModel,
    ChatTypingEventModel,
    ChatUserConnectedEventModel,
    ChatUserDisconnectedEventModel
)
from api.chats.handlers import (
    ChatMessageHandler,
    ChatTypingEventHandler,
    ChatUserConnectedEventHandler,
    ChatUserDisconnectedEventHandler
)


class ChatRabbitMQConsumer:
    """Consumer для получения сообщений из RabbitMQ и трансляции через WebSocket"""
    
    def __init__(self):
        self.is_running = False
        self.rabbitmq_messaging: IRabbitMessaging = None
    
    async def start(self):
        """Запустить consumer для прослушивания сообщений чатов"""
        if self.is_running:
            print("[CONSUMER] Chat consumer is already running")
            return
        
        self.is_running = True
        print("[CONSUMER] Starting chat messages consumer...")
        
        try:
            rabbit_factory: IRabbitFactory = ioc.get(IRabbitFactory)
            self.rabbitmq_messaging = await rabbit_factory()
            
            # Подписываемся на различные типы событий
            await self.rabbitmq_messaging.subscribe(
                ChatMessageModel,
                ChatMessageHandler()
            )
            
            await self.rabbitmq_messaging.subscribe(
                ChatTypingEventModel,
                ChatTypingEventHandler()
            )
            
            await self.rabbitmq_messaging.subscribe(
                ChatUserConnectedEventModel,
                ChatUserConnectedEventHandler()
            )
            
            await self.rabbitmq_messaging.subscribe(
                ChatUserDisconnectedEventModel,
                ChatUserDisconnectedEventHandler()
            )
            
            # Устанавливаем очереди для всех типов событий
            await self.rabbitmq_messaging.install([
                QueueSettingsModel(
                    queue_name="chat.messages",
                    prefetch_count=10
                ),
                QueueSettingsModel(
                    queue_name="chat.events.typing",
                    prefetch_count=10
                ),
                QueueSettingsModel(
                    queue_name="chat.events.user_connected",
                    prefetch_count=10
                ),
                QueueSettingsModel(
                    queue_name="chat.events.user_disconnected",
                    prefetch_count=10
                )
            ])
            
            print("[CONSUMER] Chat consumer started successfully")
            
        except Exception as e:
            print(f"[CONSUMER] Failed to start chat consumer: {e}")
            import traceback
            traceback.print_exc()
            self.is_running = False
    
    async def stop(self):
        """Остановить consumer"""
        if not self.is_running:
            return
        
        self.is_running = False
        print("[CONSUMER] Chat consumer stopped")


chat_consumer = ChatRabbitMQConsumer()

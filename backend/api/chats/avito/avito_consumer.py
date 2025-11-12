import logging
import json
import asyncio
from typing import Optional, Dict, Any
from datetime import datetime

from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.utils.ioc.ioc import ioc
from api.chats import crud

logger = logging.getLogger(__name__)

AVITO_MESSAGES_QUEUE = "avito.messages"
AVITO_MESSAGES_ROUTING_KEY = "avito.messages.*"


class AvitoMessageConsumer:
    def __init__(self):
        self.rabbit_messaging: Optional[IRabbitMessaging] = None
        self.is_running = False
    
    async def start(self):
        try:
            rabbit_factory: IRabbitFactory = await ioc.get(IRabbitFactory)()
            self.rabbit_messaging = await rabbit_factory
            
            logger.info(f"Avito consumer starting... Listening to queue: {AVITO_MESSAGES_QUEUE}")
            
            self.is_running = True
            
            await self._consume_messages()
            
        except Exception as e:
            logger.error(f"Failed to start Avito consumer: {e}", exc_info=True)
            raise
    
    async def stop(self):
        self.is_running = False
        logger.info("Avito consumer stopped")
    
    async def _consume_messages(self):
        try:
            channel = self.rabbit_messaging.channel.channels.get("publication")
            
            if not channel:
                raise Exception("Publication channel not available")
            
            exchange = await channel.declare_exchange(
                name="avito",
                type="topic",
                durable=True
            )
            
            queue = await channel.declare_queue(
                name=AVITO_MESSAGES_QUEUE,
                durable=True
            )
            
            await queue.bind(exchange, routing_key=AVITO_MESSAGES_ROUTING_KEY)
            
            logger.info(f"Queue {AVITO_MESSAGES_QUEUE} declared and bound")
            
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        try:
                            await self._process_message(message)
                        except Exception as e:
                            logger.error(f"Error processing message: {e}", exc_info=True)
        
        except Exception as e:
            logger.error(f"Error in message consumer loop: {e}", exc_info=True)
            if self.is_running:
                await asyncio.sleep(5)
                await self._consume_messages()
    
    async def _process_message(self, message):
        try:
            payload = json.loads(message.body.decode())
            
            message_type = payload.get("message_type")
            chat_id = payload.get("chat_id")
            
            logger.info(f"Processing {message_type} for chat {chat_id}")
            
            if message_type == "message_received":
                await self._handle_message_received(payload)
            elif message_type == "message_status":
                await self._handle_message_status(payload)
            elif message_type == "chat_closed":
                await self._handle_chat_closed(payload)
            else:
                logger.warning(f"Unknown message type: {message_type}")
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            raise
    
    async def _handle_message_received(self, payload: Dict[str, Any]):
        try:
            chat_id = payload.get("chat_id")
            external_message_id = payload.get("external_message_id")
            
            if not chat_id:
                logger.warning("Message received without chat_id")
                return
            logger.info(f"Message received: chat={chat_id}, external_id={external_message_id}")
            
            if payload.get("message_id"):
                try:
                    await crud.update_message(
                        payload["message_id"],
                        status="DELIVERED"
                    )
                except Exception as e:
                    logger.warning(f"Failed to update message status: {e}")
        
        except Exception as e:
            logger.error(f"Error handling message received: {e}", exc_info=True)
            raise
    
    async def _handle_message_status(self, payload: Dict[str, Any]):
        try:
            message_id = payload.get("message_id")
            status = payload.get("status") 
            
            if not message_id or not status:
                logger.warning("Status update without message_id or status")
                return
            
            await crud.update_message(message_id, status=status)
            
            logger.info(f"Message {message_id} status updated to {status}")
        
        except Exception as e:
            logger.error(f"Error handling message status: {e}", exc_info=True)
            raise
    
    async def _handle_chat_closed(self, payload: Dict[str, Any]):
        try:
            chat_id = payload.get("chat_id")
            
            if not chat_id:
                logger.warning("Chat closed without chat_id")
                return
            
            await crud.update_chat(
                chat_id,
                status="CLOSED",
                last_message_time=datetime.utcnow()
            )
            
            logger.info(f"Chat {chat_id} marked as closed")
        
        except Exception as e:
            logger.error(f"Error handling chat closed: {e}", exc_info=True)
            raise


avito_consumer = AvitoMessageConsumer()


async def start_avito_consumer():
    try:
        await avito_consumer.start()
    except Exception as e:
        logger.error(f"Failed to start Avito consumer: {e}")
        raise


async def stop_avito_consumer():
    await avito_consumer.stop()

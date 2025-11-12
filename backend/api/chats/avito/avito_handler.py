import logging
from typing import Optional, Dict, Any
from datetime import datetime
from .avito_types import AvitoWebhook
from ..producer import chat_producer
from .. import crud

logger = logging.getLogger(__name__)


class AvitoHandler:
    
    @staticmethod
    async def handle_message_event(webhook: AvitoWebhook, cashbox_id: int) -> Dict[str, Any]:
        try:
            payload = webhook.payload.value
            
            chat_id_external = payload.chat_id or ""
            user_id = payload.user_id or 0
            message_id = payload.id or ""
            message_type = payload.type or 'text' 
            
            message_content, message_text = AvitoHandler._extract_message_content(
                payload.content or {},
                message_type
            )
            
            user_name = payload.user_name or f"Avito User {user_id}"
            user_phone = payload.user_phone or None
            
            logger.info(f"Processing Avito message: {message_id} in chat {chat_id_external}")
            logger.info(f"From user: {user_name} ({user_id}), phone: {user_phone}")
            
            chat = await AvitoHandler._find_or_create_chat(
                channel_type="AVITO",
                external_chat_id=chat_id_external,
                cashbox_id=cashbox_id,
                user_id=user_id,
                webhook_data=payload
            )
            
            if not chat:
                raise Exception(f"Failed to create or find chat {chat_id_external}")
            
            contragent_result = None
            if user_phone:
                try:
                    contragent_result = await crud.chain_client(
                        chat_id=chat['id'],
                        phone=user_phone,
                        name=user_name
                    )
                    logger.info(f"Contragent linked: {contragent_result.get('contragent_id')} ({contragent_result.get('message')})")
                except Exception as e:
                    logger.warning(f"Failed to link contragent: {e}")
            
            chat_id = chat['id']
            
            logger.info(f"Chat ID: {chat_id}, Message ID: {message_id}")
            
            message = await crud.create_message_and_update_chat(
                chat_id=chat_id,
                sender_type="CLIENT",
                content=message_text,
                message_type=AvitoHandler._map_message_type(message_type),
                external_message_id=message_id,
                status="DELIVERED"
            )
            
            logger.info(f"Created message {message['id']} in chat {chat_id}")
            
            try:
                await chat_producer.send_message(chat_id, {
                    "message_id": message['id'],
                    "chat_id": chat_id,
                    "channel_type": "AVITO",
                    "external_message_id": message_id,
                    "sender_type": "CLIENT",
                    "content": message_text,
                    "message_type": message_type,
                    "created_at": datetime.utcnow().isoformat(),
                    "user_id": user_id,
                })
                logger.info(f"Sent message {message['id']} to RabbitMQ")
            except Exception as e:
                logger.error(f"Failed to send message to RabbitMQ: {e}")
            
            return {
                "success": True,
                "message": "Message processed successfully",
                "chat_id": chat_id,
                "message_id": message['id']
            }
            
        except Exception as e:
            logger.error(f"Error processing Avito webhook: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Failed to process message: {str(e)}",
                "error": str(e)
            }
    
    @staticmethod
    async def _find_or_create_chat(
        channel_type: str,
        external_chat_id: str,
        cashbox_id: int,
        user_id: int,
        webhook_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        try:
            channel = await crud.get_channel_by_type(channel_type)
            
            if not channel:
                logger.warning(f"Channel {channel_type} not found, creating new one")
                channel = await crud.create_channel(
                    name=channel_type,
                    type=channel_type,
                    description=f"{channel_type} integration channel"
                )
            
            existing_chat = await crud.get_chat_by_external_id(
                channel_id=channel['id'],
                external_chat_id=external_chat_id,
                cashbox_id=cashbox_id
            )
            
            if existing_chat:
                logger.info(f"Found existing chat: {existing_chat['id']}")
                return existing_chat
            
            logger.info(f"Creating new chat for Avito chat {external_chat_id}")
            new_chat = await crud.create_chat(
                channel_id=channel['id'],
                cashbox_id=cashbox_id,
                external_chat_id=external_chat_id,
                phone=None,  
                name=f"Avito Chat {external_chat_id[:8]}"
            )
            
            return new_chat
            
        except Exception as e:
            logger.error(f"Failed to find/create chat: {e}", exc_info=True)
            return None
    
    @staticmethod
    def _extract_message_content(content: Dict[str, Any], message_type: str) -> tuple[Dict[str, Any], str]:
        if not content:
            return {}, ""
        
        message_content = content.copy() if isinstance(content, dict) else {}
        message_text = ""
        
        if message_type == 'text':
            message_text = content.get('text', '') if isinstance(content, dict) else str(content)
        
        elif message_type == 'image':
            if isinstance(content, dict) and 'image' in content:
                message_content = content['image']
                message_text = f"[Image: {content['image'].get('url', 'No URL')}]"
            else:
                message_text = "[Image message]"
        
        elif message_type == 'voice':
            if isinstance(content, dict) and 'voice' in content:
                message_content = content['voice']
                message_text = f"[Voice message: {content['voice'].get('duration', 'unknown')}s]"
            else:
                message_text = "[Voice message]"
        
        elif message_type == 'video':
            if isinstance(content, dict) and 'video' in content:
                message_content = content['video']
                message_text = f"[Video: {content['video'].get('url', 'No URL')}]"
            else:
                message_text = "[Video message]"
        
        elif message_type == 'file' or message_type == 'document':
            if isinstance(content, dict) and 'file' in content:
                message_content = content['file']
                message_text = f"[File: {content['file'].get('name', 'document')}]"
            else:
                message_text = "[Document]"
        
        elif message_type == 'link':
            if isinstance(content, dict) and 'link' in content:
                message_content = content['link']
                message_text = content['link'].get('text') or content['link'].get('url', '[Link]')
            else:
                message_text = "[Link]"
        
        elif message_type == 'location':
            if isinstance(content, dict) and 'location' in content:
                message_content = content['location']
                message_text = f"[Location: {content['location'].get('latitude')}, {content['location'].get('longitude')}]"
            else:
                message_text = "[Location]"
        
        elif message_type == 'item':
            if isinstance(content, dict) and 'item' in content:
                message_content = content['item']
                message_text = f"[Item: {content['item'].get('title', 'No title')}]"
            else:
                message_text = "[Avito item]"
        
        elif message_type == 'system':
            message_text = content.get('text', '[System message]') if isinstance(content, dict) else '[System message]'
        
        else:
            message_text = str(content) if not isinstance(content, dict) else "[Unknown message type]"
        
        return message_content, message_text
    
    @staticmethod
    def _map_message_type(avito_type: str) -> str:
        mapping = {
            'text': 'TEXT',
            'image': 'IMAGE',
            'voice': 'VOICE',
            'video': 'VIDEO',
            'file': 'DOCUMENT',
            'item': 'DOCUMENT', 
            'location': 'TEXT',  
            'link': 'TEXT',  
            'appCall': 'TEXT', 
            'system': 'SYSTEM',
        }
        return mapping.get(avito_type, 'TEXT')
    
    @staticmethod
    async def handle_webhook_event(
        webhook: AvitoWebhook,
        cashbox_id: int
    ) -> Dict[str, Any]:
        
        event_type = webhook.payload.type
        
        if event_type == 'message':
            return await AvitoHandler.handle_message_event(webhook, cashbox_id)
        
        elif event_type == 'status':
            logger.info(f"Status event received (not implemented): {webhook.id}")
            return {"success": True, "message": "Status event received (not implemented)"}
        
        elif event_type == 'typing':
            logger.info(f"Typing event received (not implemented): {webhook.id}")
            return {"success": True, "message": "Typing event received"}
        
        else:
            logger.warning(f"Unknown event type: {event_type}")
            return {"success": False, "message": f"Unknown event type: {event_type}"}

    @staticmethod
    async def sync_messages_from_avito(
        client: "AvitoClient",
        chat_id: int,
        cashbox_id: int,
        external_chat_id: str
    ) -> Dict[str, Any]:
        try:
            logger.info(f"Starting sync for chat {chat_id} (external: {external_chat_id})")
            
            avito_messages = await client.sync_messages(external_chat_id)
            
            if not avito_messages:
                logger.info(f"No messages to sync for chat {chat_id}")
                return {
                    "success": True,
                    "synced_count": 0,
                    "new_messages": 0,
                    "updated_messages": 0,
                    "errors": []
                }
            
            synced_count = len(avito_messages)
            new_messages = 0
            updated_messages = 0
            errors = []
            
            for avito_msg in avito_messages:
                try:
                    message_id = avito_msg.get('id')
                    
                    existing_message = await crud.get_message_by_external_id(
                        chat_id=chat_id,
                        external_message_id=message_id
                    )
                    
                    if existing_message:
                        logger.debug(f"Message {message_id} already exists in chat {chat_id}")
                        updated_messages += 1
                        continue
                    
                    message_text = avito_msg.get('content', {}).get('text', '') if avito_msg.get('content') else ''
                    message_type = avito_msg.get('type', 'text')
                    sender_id = avito_msg.get('authorId')
                    
                    sender_type = "CLIENT"
                    if sender_id and str(sender_id) == "0":
                        sender_type = "OPERATOR"
                    
                    message = await crud.create_message_and_update_chat(
                        chat_id=chat_id,
                        sender_type=sender_type,
                        content=message_text,
                        message_type=AvitoHandler._map_message_type(message_type),
                        external_message_id=message_id,
                        status="DELIVERED"
                    )
                    
                    logger.info(f"Synced message {message_id} to chat {chat_id}")
                    new_messages += 1
                    
                except Exception as msg_error:
                    logger.error(f"Error syncing message {avito_msg.get('id')}: {msg_error}")
                    errors.append({
                        "message_id": avito_msg.get('id'),
                        "error": str(msg_error)
                    })
            
            logger.info(f"Sync completed for chat {chat_id}: {new_messages} new, {updated_messages} updated")
            
            return {
                "success": True,
                "synced_count": synced_count,
                "new_messages": new_messages,
                "updated_messages": updated_messages,
                "errors": errors
            }
        
        except Exception as e:
            logger.error(f"Failed to sync messages for chat {chat_id}: {e}", exc_info=True)
            return {
                "success": False,
                "synced_count": 0,
                "new_messages": 0,
                "updated_messages": 0,
                "errors": [{"error": str(e)}]
            }

    @staticmethod
    async def handle_status_event(webhook: AvitoWebhook, cashbox_id: int) -> Dict[str, Any]:
        try:
            payload = webhook.payload.value
            
            message_id = payload.get('id')
            chat_id_external = payload.chat_id or ""
            status = payload.get('status')  
            
            logger.info(f"Status event for message {message_id} in chat {chat_id_external}: {status}")
            
            chat = await crud.get_chat_by_external_id(
                channel_id=None, 
                external_chat_id=chat_id_external,
                cashbox_id=cashbox_id
            )
            
            if not chat:
                logger.warning(f"Chat {chat_id_external} not found for status event")
                return {
                    "success": False,
                    "message": f"Chat {chat_id_external} not found"
                }
            
            message = await crud.get_message_by_external_id(
                chat_id=chat['id'],
                external_message_id=message_id
            )
            
            if not message:
                logger.warning(f"Message {message_id} not found in chat {chat['id']}")
                return {
                    "success": False,
                    "message": f"Message {message_id} not found"
                }
            
            if status == 'read':
                new_status = "READ"
            elif status == 'deleted':
                new_status = "DELETED"
            else:
                new_status = status.upper() if status else "UNKNOWN"
            
            await crud.update_message(message['id'], status=new_status)
            
            logger.info(f"Updated message {message['id']} status to {new_status}")
            
            return {
                "success": True,
                "message": f"Status updated to {new_status}",
                "message_id": message['id']
            }
        
        except Exception as e:
            logger.error(f"Error processing status event: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Error processing status event: {str(e)}"
            }

    @staticmethod
    async def handle_typing_event(webhook: AvitoWebhook, cashbox_id: int) -> Dict[str, Any]:
        try:
            payload = webhook.payload.value
            
            chat_id_external = payload.chat_id or ""
            is_typing = payload.get('isTyping', False)
            user_id = payload.get('authorId')
            
            logger.info(f"Typing event in chat {chat_id_external} from user {user_id}: {is_typing}")
            
            chat = await crud.get_chat_by_external_id(
                channel_id=None,
                external_chat_id=chat_id_external,
                cashbox_id=cashbox_id
            )
            
            if not chat:
                logger.warning(f"Chat {chat_id_external} not found for typing event")
                return {
                    "success": False,
                    "message": f"Chat {chat_id_external} not found"
                }
            
            logger.info(f"User {user_id} is {'typing' if is_typing else 'not typing'} in chat {chat['id']}")
            
            return {
                "success": True,
                "message": f"Typing event processed for chat {chat['id']}",
                "chat_id": chat['id'],
                "user_typing": is_typing
            }
        
        except Exception as e:
            logger.error(f"Error processing typing event: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Error processing typing event: {str(e)}"
            }

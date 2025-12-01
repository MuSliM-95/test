from fastapi import APIRouter, Request
import logging
from api.chats.avito.avito_webhook import process_avito_webhook
from api.chats.avito.avito_handler import AvitoHandler
from api.chats.avito.schemas import AvitoWebhookResponse
from database.db import channel_credentials, channels, database
from sqlalchemy import select, and_

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/avito", tags=["avito-webhook"])

@router.post("/hook", response_model=AvitoWebhookResponse)
async def receive_avito_webhook_default(request: Request):
    try:
        body = await request.body()
        
        signature_header = request.headers.get("X-Avito-Signature")
        
        is_valid, webhook_data, cashbox_id = await process_avito_webhook(
            body,
            signature_header
        )
        
        if not is_valid:
            logger.warning("Invalid webhook received")
            return {
                "success": False,
                "message": "Invalid webhook signature or structure"
            }
        
        if not cashbox_id:
            logger.warning("Could not determine cashbox_id from webhook")
            return {
                "success": False,
                "message": "Could not determine cashbox_id"
            }
        
        try:
            from api.chats.avito.avito_types import AvitoWebhook
            webhook = AvitoWebhook(**webhook_data)
        except Exception as e:
            logger.error(f"Failed to parse webhook data into AvitoWebhook model: {e}")
            return {
                "success": False,
                "message": f"Invalid webhook structure: {str(e)}"
            }
        
        user_id = None
        if hasattr(webhook.payload, 'value') and webhook.payload.value:
            user_id = getattr(webhook.payload.value, 'user_id', None)
        
        if user_id:
            channels_query = select([
                channel_credentials.c.channel_id
            ]).select_from(
                channel_credentials.join(
                    channels,
                    channel_credentials.c.channel_id == channels.c.id
                )
            ).where(
                and_(
                    channels.c.type == 'AVITO',
                    channels.c.is_active.is_(True),
                    channel_credentials.c.avito_user_id == user_id,
                    channel_credentials.c.cashbox_id == cashbox_id,
                    channel_credentials.c.is_active.is_(True)
                )
            )
            
            channels_result = await database.fetch_all(channels_query)
            
            if channels_result and len(channels_result) > 1:
                results = []
                for channel_row in channels_result:
                    channel_id = channel_row['channel_id']
                    logger.info(f"Processing webhook for channel {channel_id} (user_id: {user_id})")
                    result = await AvitoHandler.handle_webhook_event(webhook, cashbox_id, channel_id)
                    results.append(result)
                
                success_result = next((r for r in results if r.get('success')), results[-1] if results else None)
                result = success_result if success_result else {
                    "success": False,
                    "message": "Failed to process webhook for any channel"
                }
            else:
                result = await AvitoHandler.handle_webhook_event(webhook, cashbox_id)
        else:
            result = await AvitoHandler.handle_webhook_event(webhook, cashbox_id)
        
        return {
            "success": result.get("success", False),
            "message": result.get("message", "Event processed"),
            "chat_id": result.get("chat_id"),
            "message_id": result.get("message_id")
        }
    
    except Exception as e:
        logger.error(f"Error processing Avito webhook: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Error: {str(e)}"
        }


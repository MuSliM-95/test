import hmac
import hashlib
import json
import logging
import os
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

AVITO_WEBHOOK_SECRET = "" 


def verify_webhook_signature(
    request_body: bytes,
    signature_header: str,
    webhook_secret: Optional[str] = None
) -> bool:
    try:
        secret = webhook_secret or AVITO_WEBHOOK_SECRET
        
        if not secret:
            logger.warning("Webhook secret not configured - skipping signature verification (dev mode)")
            return True  

        calculated_signature = hmac.new(
            secret.encode(),
            request_body,
            hashlib.sha256
        ).hexdigest()
        
        is_valid = hmac.compare_digest(calculated_signature, signature_header)
        
        if not is_valid:
            logger.warning(f"Invalid webhook signature. Expected: {calculated_signature}, Got: {signature_header}")
        
        return is_valid
        
    except Exception as e:
        logger.error(f"Error verifying webhook signature: {e}", exc_info=True)
        return False


def validate_webhook_structure(webhook_data: Dict[str, Any]) -> bool:
    required_fields = ['id', 'timestamp', 'payload']
    
    for field in required_fields:
        if field not in webhook_data:
            logger.error(f"Missing required field in webhook: {field}")
            return False
    
    return True


def extract_cashbox_id_from_webhook(webhook_data: Dict[str, Any]) -> Optional[int]:
    try:
        payload = webhook_data.get('payload', {})
        
        avito_identifier = (
            payload.get('user_id') or
            payload.get('account_id') or
            payload.get('seller_id') or
            webhook_data.get('user_id')
        )
        
        if not avito_identifier:
            logger.warning("Could not extract Avito identifier from webhook - cannot lookup cashbox_id")
            return None
        
        logger.info(f"Webhook contains Avito identifier: {avito_identifier}, will lookup cashbox in handler")
        
        return None
        
    except (ValueError, TypeError) as e:
        logger.error(f"Error extracting cashbox_id from webhook: {e}")
        return None


async def get_cashbox_id_for_avito_webhook(webhook_data: Dict[str, Any]) -> Optional[int]:
    from database.db import database, channels, chats, channel_credentials
    from sqlalchemy import select, and_
    
    try:
        payload_value = webhook_data.get('payload', {}).get('value', {})
        if not payload_value:
            logger.warning("No payload.value in webhook data")
            return None
        
        external_chat_id = payload_value.get('chat_id') or payload_value.get('chatId')
        user_id = payload_value.get('user_id') or payload_value.get('userId')
        
        if not external_chat_id and not user_id:
            logger.warning("Could not extract chat_id or user_id from webhook")
            return None
        
        if external_chat_id:
            query = select([
                chats.c.id,
                chats.c.cashbox_id,
                chats.c.channel_id
            ]).select_from(
                chats.join(
                    channels,
                    chats.c.channel_id == channels.c.id
                )
            ).where(
                and_(
                    channels.c.type == 'AVITO',
                    channels.c.is_active.is_(True),
                    chats.c.external_chat_id == str(external_chat_id)
                )
            ).limit(1)
            
            existing_chat = await database.fetch_one(query)
            
            if existing_chat:
                cashbox_id = existing_chat['cashbox_id']
                channel_id = existing_chat['channel_id']
                logger.info(f"Found cashbox_id {cashbox_id} and channel_id {channel_id} from existing chat {existing_chat['id']} by external_chat_id {external_chat_id}")
                return cashbox_id
        
        if user_id:
            query = select([
                channel_credentials.c.cashbox_id
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
                    channel_credentials.c.is_active.is_(True)
                )
            ).limit(1)
            
            creds = await database.fetch_one(query)
            if creds:
                cashbox_id = creds['cashbox_id']
                logger.info(f"Found cashbox_id {cashbox_id} from channel_credentials by avito_user_id {user_id}")
                return cashbox_id
            else:
                logger.debug(f"Could not find channel_credentials by user_id {user_id}")
        
        logger.warning(
            f"Could not determine cashbox_id from webhook. "
            f"external_chat_id={external_chat_id}, user_id={user_id}. "
            f"Chat may need to be created first via API or webhook URL should include cashbox_id parameter."
        )
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting cashbox_id for Avito webhook: {e}", exc_info=True)
        return None



async def process_avito_webhook(
    request_body: bytes,
    signature_header: Optional[str] = None,
    webhook_secret: Optional[str] = None
) -> tuple[bool, Dict[str, Any], Optional[int]]:
    try:
        if signature_header:
            if not verify_webhook_signature(request_body, signature_header, webhook_secret):
                logger.error("Webhook signature verification failed")
                return False, {}, None
        
        try:
            webhook_data = json.loads(request_body.decode())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to parse webhook JSON: {e}")
            return False, {}, None
        
        if not validate_webhook_structure(webhook_data):
            logger.error("Invalid webhook structure")
            return False, webhook_data, None
        
        cashbox_id = await get_cashbox_id_for_avito_webhook(webhook_data)
        
        if not cashbox_id:
            logger.error("Could not determine cashbox_id - no active Avito credentials found")
            return False, webhook_data, None
        
        logger.info(f"Valid webhook received from cashbox {cashbox_id}")
        return True, webhook_data, cashbox_id
        
    except Exception as e:
        logger.error(f"Error processing webhook: {e}", exc_info=True)
        return False, {}, None

import hmac
import hashlib
import json
import logging
import os
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

AVITO_WEBHOOK_SECRET = os.getenv("AVITO_WEBHOOK_SECRET", "")  # Must be set in production


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
    from database.db import database
    
    try:
        payload = webhook_data.get('payload', {})
        
        avito_identifier = (
            payload.get('user_id') or
            payload.get('account_id') or
            payload.get('seller_id')
        )
        
        if not avito_identifier:
            logger.warning("Could not extract Avito identifier from webhook")
            return None
        
        channel = await database.fetch_one(
            "SELECT id FROM channels WHERE type = 'AVITO' AND is_active = TRUE"
        )
        
        if not channel:
            logger.error("AVITO channel not found in database")
            return None
        
        credentials = await database.fetch_one(
            """
            SELECT DISTINCT cashbox_id 
            FROM channel_credentials 
            WHERE channel_id = :channel_id 
            AND is_active = TRUE
            LIMIT 1
            """,
            {"channel_id": channel['id']}
        )
        
        if credentials:
            logger.info(f"Found cashbox_id {credentials['cashbox_id']} for Avito identifier {avito_identifier}")
            return credentials['cashbox_id']
        else:
            logger.warning(f"No active Avito credentials found for identifier {avito_identifier}")
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

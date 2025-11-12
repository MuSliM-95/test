from fastapi import APIRouter, Depends, HTTPException, Request
from datetime import datetime
import logging

from api.chats.auth import get_current_user
from api.chats.avito.schemas import (
    AvitoCredentialsCreate,
    AvitoWebhookResponse,
    AvitoSyncResponse,
    AvitoSendMessageRequest,
    AvitoSendMessageResponse,
    AvitoConnectResponse
)
from api.chats.avito.avito_handler import AvitoHandler
from api.chats.avito.avito_client import AvitoClient
from api.chats.avito.avito_factory import (
    create_avito_client,
    validate_avito_credentials,
    save_token_callback,
    _encrypt_credential
)
from api.chats.avito.avito_webhook import process_avito_webhook
from api.chats import crud
from database.db import database, channels

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chats/avito", tags=["chats-avito"])


@router.post("/connect", response_model=AvitoConnectResponse)
async def connect_avito_channel(
    credentials: AvitoCredentialsCreate,
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        is_valid = await validate_avito_credentials(
            api_key=credentials.api_key,
            api_secret=credentials.api_secret,
            access_token=credentials.access_token
        )
        
        if not is_valid:
            raise HTTPException(
                status_code=401,
                detail="Avito credentials validation failed - check your credentials"
            )
        
        avito_channel = await database.fetch_one(
            channels.select().where(channels.c.type == "AVITO")
        )
        
        if not avito_channel:
            channel_id = await database.execute(
                channels.insert().values(
                    name="Avito",
                    type="AVITO",
                    description="Avito White API Integration",
                    is_active=True
                )
            )
            avito_channel = await crud.get_channel(channel_id)
        
        channel_id = avito_channel['id']
        
        encrypted_api_key = _encrypt_credential(credentials.api_key)
        encrypted_api_secret = _encrypt_credential(credentials.api_secret)
        encrypted_access_token = _encrypt_credential(credentials.access_token)
        
        existing = await database.fetch_one(
            database.table("channel_credentials").select().where(
                (database.table("channel_credentials").c.channel_id == channel_id) &
                (database.table("channel_credentials").c.cashbox_id == cashbox_id) &
                (database.table("channel_credentials").c.is_active.is_(True))
            )
        )
        
        if existing:
            await database.execute(
                database.table("channel_credentials").update().where(
                    database.table("channel_credentials").c.id == existing['id']
                ).values(
                    api_key=encrypted_api_key,
                    api_secret=encrypted_api_secret,
                    access_token=encrypted_access_token,
                    is_active=True,
                    updated_at=datetime.utcnow()
                )
            )
        else:
            await database.execute(
                database.table("channel_credentials").insert().values(
                    channel_id=channel_id,
                    cashbox_id=cashbox_id,
                    api_key=encrypted_api_key,
                    api_secret=encrypted_api_secret,
                    access_token=encrypted_access_token,
                    is_active=True,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
            )
        
        return {
            "success": True,
            "message": f"Avito канал успешно подключен к кабинету {cashbox_id}",
            "channel_id": channel_id,
            "cashbox_id": cashbox_id
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ошибка подключения: {str(e)}")


@router.post("/webhooks/message", response_model=AvitoWebhookResponse)
async def receive_avito_webhook(request: Request):
    try:
        body = await request.body()
        
        signature_header = request.headers.get("X-Avito-Signature")
        
        is_valid, webhook_data, cashbox_id = await process_avito_webhook(
            body,
            signature_header
        )
        
        if not is_valid:
            logger.error("Invalid webhook received")
            return {
                "success": False,
                "message": "Invalid webhook signature or structure"
            }
        
        if not cashbox_id:
            logger.error("Could not determine cashbox_id from webhook")
            return {
                "success": False,
                "message": "Could not determine cashbox_id"
            }
        
        result = await AvitoHandler.handle_webhook_event(webhook_data, cashbox_id)
        
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


@router.post("/sync", response_model=AvitoSyncResponse)
async def sync_avito_messages(
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channel = await database.fetch_one(
            channels.select().where(channels.c.type == "AVITO")
        )
        
        if not avito_channel:
            raise HTTPException(status_code=404, detail="Avito channel not configured")
        
        client = await create_avito_client(
            channel_id=avito_channel['id'],
            cashbox_id=cashbox_id,
            on_token_refresh=lambda token_data: save_token_callback(
                avito_channel['id'],
                cashbox_id,
                token_data
            )
        )
        
        if not client:
            raise HTTPException(
                status_code=500,
                detail="Could not create Avito API client. Check credentials."
            )
        
        chats = await crud.get_chats(
            cashbox_id=cashbox_id,
            channel_id=avito_channel['id'],
            limit=100
        )
        
        synced_count = 0
        new_messages = 0
        updated_messages = 0
        errors = []
        
        for chat in chats:
            try:
                external_chat_id = chat.get('external_chat_id')
                if not external_chat_id:
                    logger.warning(f"Chat {chat['id']} has no external_chat_id")
                    continue

                avito_messages = await client.sync_messages(
                    chat_id=external_chat_id
                )
                
                logger.info(f"Synced {len(avito_messages)} messages from Avito chat {external_chat_id}")
                
                for avito_msg in avito_messages:
                    try:
                        existing = await database.fetch_one(
                            database.table("chat_messages").select().where(
                                database.table("chat_messages").c.external_message_id == avito_msg.get('id')
                            )
                        )
                        
                        if existing:
                            updated_messages += 1
                            continue
                        
                        await crud.create_message_and_update_chat(
                            chat_id=chat['id'],
                            sender_type="CLIENT",
                            content=avito_msg.get('text', ''),
                            message_type=AvitoHandler._map_message_type(
                                avito_msg.get('type', 'text')
                            ),
                            external_message_id=avito_msg.get('id'),
                            status="DELIVERED"
                        )
                        new_messages += 1
                    
                    except Exception as e:
                        logger.error(f"Failed to save message {avito_msg.get('id')}: {e}")
                        errors.append(f"Failed to save message: {str(e)}")
                
                synced_count += 1
            
            except Exception as e:
                logger.error(f"Failed to sync chat {chat['id']}: {e}")
                errors.append(f"Failed to sync chat {chat['id']}: {str(e)}")
        
        return {
            "synced_count": synced_count,
            "new_messages": new_messages,
            "updated_messages": updated_messages,
            "errors": errors
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Sync error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Sync error: {str(e)}")


@router.post("/send", response_model=AvitoSendMessageResponse)
async def send_avito_message(
    request: AvitoSendMessageRequest,
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        chat = await crud.get_chat(request.chat_id)
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        if chat['cashbox_id'] != cashbox_id:
            raise HTTPException(status_code=403, detail="Access denied - chat belongs to another cashbox")
        
        channel = await crud.get_channel(chat['channel_id'])
        if not channel or channel['type'] != 'AVITO':
            raise HTTPException(status_code=400, detail="Chat is not from Avito channel")
        
        message = await crud.create_message_and_update_chat(
            chat_id=request.chat_id,
            sender_type="OPERATOR",
            content=request.content,
            message_type=request.message_type or "TEXT",
            status="SENT"
        )
        
        external_message_id = None
        try:
            client = await create_avito_client(
                channel_id=channel['id'],
                cashbox_id=cashbox_id,
                on_token_refresh=lambda token_data: save_token_callback(
                    channel['id'],
                    cashbox_id,
                    token_data
                )
            )
            
            if not client:
                logger.warning(f"Could not create Avito client for channel {channel['id']}, cashbox {cashbox_id}")
                await crud.update_message(message['id'], status="FAILED")
                raise HTTPException(
                    status_code=500,
                    detail="Could not create Avito API client. Check credentials."
                )
            
            avito_message = await client.send_message(
                chat_id=chat['external_chat_id'],
                text=request.content
            )
            
            external_message_id = avito_message.get('id')
            
            await crud.update_message(
                message['id'],
                external_message_id=external_message_id,
                status="DELIVERED"
            )
            
            logger.info(f"Message {message['id']} sent to Avito, external_id: {external_message_id}")
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to send message to Avito: {e}", exc_info=True)
            try:
                await crud.update_message(message['id'], status="FAILED")
            except Exception as update_error:
                logger.error(f"Failed to update message status to FAILED: {update_error}")
            
            raise HTTPException(
                status_code=500,
                detail=f"Failed to send message to Avito API: {str(e)}"
            )
        
        return {
            "success": True,
            "message_id": str(message['id']),
            "external_message_id": external_message_id
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Send error: {str(e)}")


@router.get("/status")
async def get_avito_status(
    user = Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        
        avito_channel = await database.fetch_one(
            channels.select().where(channels.c.type == "AVITO")
        )
        
        if not avito_channel:
            return {
                "connected": False,
                "message": "Avito channel not configured in system"
            }
        
        credentials = await database.fetch_one(
            database.table("channel_credentials").select().where(
                (database.table("channel_credentials").c.channel_id == avito_channel['id']) &
                (database.table("channel_credentials").c.cashbox_id == cashbox_id) &
                (database.table("channel_credentials").c.is_active.is_(True))
            )
        )
        
        if not credentials:
            return {
                "connected": False,
                "channel_id": avito_channel['id'],
                "channel_name": avito_channel['name'],
                "cashbox_id": cashbox_id,
                "message": "Avito channel configured but no credentials for this cashbox"
            }
        
        try:
            client = await create_avito_client(
                channel_id=avito_channel['id'],
                cashbox_id=cashbox_id,
                on_token_refresh=lambda token_data: save_token_callback(
                    avito_channel['id'],
                    cashbox_id,
                    token_data
                )
            )
            
            if not client:
                return {
                    "connected": False,
                    "channel_id": avito_channel['id'],
                    "channel_name": avito_channel['name'],
                    "cashbox_id": cashbox_id,
                    "message": "Credentials are invalid or expired"
                }
            
            is_valid = await client.validate_token()
            
            return {
                "connected": is_valid,
                "channel_id": avito_channel['id'],
                "channel_name": avito_channel['name'],
                "cashbox_id": cashbox_id,
                "message": "Avito channel is connected" if is_valid else "Token validation failed"
            }
        
        except Exception as e:
            logger.warning(f"Failed to validate Avito token: {e}")
            return {
                "connected": False,
                "channel_id": avito_channel['id'],
                "channel_name": avito_channel['name'],
                "cashbox_id": cashbox_id,
                "message": f"Failed to validate connection: {str(e)}"
            }
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Status error: {str(e)}")

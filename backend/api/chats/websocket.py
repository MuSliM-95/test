from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
import json
from typing import Dict, List
from datetime import datetime
from api.chats.producer import chat_producer
from api.chats import crud
from api.chats.auth import get_current_user

router = APIRouter(prefix="/chats", tags=["chats-ws"])

class ChatConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, List[WebSocket]] = {}
    
    async def connect(self, chat_id: int, websocket: WebSocket):
        await websocket.accept()
        if chat_id not in self.active_connections:
            self.active_connections[chat_id] = []
        self.active_connections[chat_id].append(websocket)
        print(f"Client connected to chat {chat_id}. Total clients: {len(self.active_connections[chat_id])}")
    
    async def disconnect(self, chat_id: int, websocket: WebSocket):
        if chat_id in self.active_connections:
            try:
                self.active_connections[chat_id].remove(websocket)
            except Exception:
                pass
            if not self.active_connections[chat_id]:
                del self.active_connections[chat_id]
            print(f"Client disconnected from chat {chat_id}")
        else:
            print("Client disconnect failed - chat not in active connections")
    
    async def broadcast_to_chat(self, chat_id: int, message: dict):
        if chat_id in self.active_connections:
            disconnected_clients = []
            for i, connection in enumerate(self.active_connections[chat_id]):
                try:
                    await connection.send_json(message)
                except Exception as e:
                    print(f"Failed to send to client in chat {chat_id}: {e}")
                    disconnected_clients.append(i)
            
            for i in reversed(disconnected_clients):
                try:
                    self.active_connections[chat_id].pop(i)
                except Exception:
                    pass

chat_manager = ChatConnectionManager()

@router.websocket("/ws/{chat_id}/")
async def websocket_chat(chat_id: int, websocket: WebSocket, token: str = Query(...)):
    """WebSocket для чатов с аутентификацией и RabbitMQ"""
    try:
        try:
            user = await get_current_user(token)
        except Exception as e:
            await websocket.accept()
            await websocket.send_json({"error": "Unauthorized", "detail": str(e)})
            await websocket.close(code=1008)
            print(f"Authentication failed for chat {chat_id}: {e}")
            return
        
        chat = await crud.get_chat(chat_id)
        if not chat:
            await websocket.accept()
            await websocket.send_json({"error": "Chat not found"})
            await websocket.close(code=1008)
            print(f"Chat not found for WebSocket connection: {chat_id}")
            return
        
        if chat.cashbox_id != user.cashbox_id:
            await websocket.accept()
            await websocket.send_json({"error": "Access denied"})
            await websocket.close(code=1008)
            print(f"Access denied - cashbox mismatch for chat {chat_id}")
            return
        
        await chat_manager.connect(chat_id, websocket)
        
        while True:
            data = await websocket.receive_text()
            message_data = json.loads(data)
            
            sender_type = message_data.get("sender_type", "OPERATOR").upper()
            message_type = message_data.get("message_type", "TEXT").upper()
            
            try:
                db_message = await crud.create_message_and_update_chat(
                    chat_id=chat_id,
                    sender_type=sender_type,
                    content=message_data.get("content", ""),
                    message_type=message_type,
                    status="SENT",
                    source="web"
                )
                print(f"Message saved to DB for chat {chat_id}: {db_message.id}")
            except Exception as e:
                print(f"Failed to save message to DB for chat {chat_id}: {e}")
                await websocket.send_json({"error": "Failed to save message", "detail": str(e)})
                continue
            
            try:
                await chat_producer.send_message(chat_id, {
                    "message_id": db_message.id,
                    "sender_type": sender_type,
                    "content": message_data.get("content", ""),
                    "message_type": message_type,
                    "timestamp": datetime.utcnow().isoformat()
                })
            except Exception as e:
                print(f"Failed to send message to RabbitMQ for chat {chat_id}: {e}")
            
            response = {
                "type": "message",
                "message_id": db_message.id,
                "chat_id": chat_id,
                "sender_type": sender_type,
                "content": message_data.get("content", ""),
                "message_type": message_type,
                "status": "DELIVERED",
                "timestamp": datetime.utcnow().isoformat()
            }
            await chat_manager.broadcast_to_chat(chat_id, response)
    
    except WebSocketDisconnect:
        await chat_manager.disconnect(chat_id, websocket)
    except Exception as e:
        print(f"Critical error in WebSocket for chat {chat_id}: {e}")
        await chat_manager.disconnect(chat_id, websocket)
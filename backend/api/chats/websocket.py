from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
import json
from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass
from api.chats.producer import chat_producer
from api.chats import crud
from api.chats.auth import get_current_user

router = APIRouter(prefix="/chats", tags=["chats-ws"])


@dataclass
class ChatConnectionInfo:
    """Информация о подключенном пользователе"""
    websocket: WebSocket
    user_id: int
    user_type: str  # OPERATOR или CLIENT
    connected_at: datetime


class ChatConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, List[ChatConnectionInfo]] = {}
    
    async def connect(self, chat_id: int, websocket: WebSocket, user_id: int, user_type: str):
        """Подключить пользователя к чату"""
        await websocket.accept()
        if chat_id not in self.active_connections:
            self.active_connections[chat_id] = []
        
        connection_info = ChatConnectionInfo(
            websocket=websocket,
            user_id=user_id,
            user_type=user_type,
            connected_at=datetime.utcnow()
        )
        
        self.active_connections[chat_id].append(connection_info)
        print(f"User {user_id} ({user_type}) connected to chat {chat_id}. Total clients: {len(self.active_connections[chat_id])}")
    
    async def disconnect(self, chat_id: int, websocket: WebSocket) -> Optional[ChatConnectionInfo]:
        """Отключить пользователя от чата и вернуть информацию о подключении"""
        if chat_id in self.active_connections:
            connection_info = None
            for conn_info in self.active_connections[chat_id]:
                if conn_info.websocket == websocket:
                    connection_info = conn_info
                    self.active_connections[chat_id].remove(conn_info)
                    break
            
            if not self.active_connections[chat_id]:
                del self.active_connections[chat_id]
            
            if connection_info:
                print(f"User {connection_info.user_id} ({connection_info.user_type}) disconnected from chat {chat_id}")
                return connection_info
            else:
                print(f"Client disconnect failed - connection not found in chat {chat_id}")
                return None
        else:
            print(f"Client disconnect failed - chat {chat_id} not in active connections")
            return None
    
    def get_connection_info(self, chat_id: int, websocket: WebSocket) -> Optional[ChatConnectionInfo]:
        """Получить информацию о подключении по WebSocket"""
        if chat_id in self.active_connections:
            for conn_info in self.active_connections[chat_id]:
                if conn_info.websocket == websocket:
                    return conn_info
        return None
    
    def get_connected_users(self, chat_id: int) -> List[Dict]:
        """Получить список подключенных пользователей в чате"""
        if chat_id not in self.active_connections:
            return []
        
        users = []
        for conn_info in self.active_connections[chat_id]:
            users.append({
                "user_id": conn_info.user_id,
                "user_type": conn_info.user_type,
                "connected_at": conn_info.connected_at.isoformat()
            })
        
        return users
    
    async def broadcast_to_chat(self, chat_id: int, message: dict):
        """Транслировать сообщение всем подключенным к чату"""
        if chat_id in self.active_connections:
            disconnected_clients = []
            for i, conn_info in enumerate(self.active_connections[chat_id]):
                try:
                    await conn_info.websocket.send_json(message)
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
        
        # Определяем тип пользователя (OPERATOR или CLIENT)
        user_type = "OPERATOR" if user.is_owner else "OPERATOR"  # Пока все через WebSocket - операторы
        
        # Подключаем пользователя
        await chat_manager.connect(chat_id, websocket, user.user, user_type)
        
        # Отправляем событие подключения через RabbitMQ
        try:
            await chat_producer.send_user_connected_event(chat_id, user.user, user_type)
        except Exception as e:
            print(f"Failed to send user connected event to RabbitMQ: {e}")
        
        while True:
            data = await websocket.receive_text()
            message_data = json.loads(data)
            
            event_type = message_data.get("type", "message")
            
            # Обработка разных типов событий
            if event_type == "message":
                # Обработка обычных сообщений
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
            
            elif event_type == "typing":
                # Обработка события печати
                is_typing = message_data.get("is_typing", False)
                
                # Отправляем событие через RabbitMQ
                try:
                    await chat_producer.send_typing_event(chat_id, user.user, user_type, is_typing)
                except Exception as e:
                    print(f"Failed to send typing event to RabbitMQ: {e}")
            
            elif event_type == "get_users":
                # Получение списка подключенных пользователей
                users = chat_manager.get_connected_users(chat_id)
                await websocket.send_json({
                    "type": "users_list",
                    "chat_id": chat_id,
                    "users": users,
                    "timestamp": datetime.utcnow().isoformat()
                })
            
            else:
                await websocket.send_json({
                    "error": "Unknown event type",
                    "type": event_type
                })
    
    except WebSocketDisconnect:
        connection_info = await chat_manager.disconnect(chat_id, websocket)
        if connection_info:
            try:
                await chat_producer.send_user_disconnected_event(chat_id, connection_info.user_id, connection_info.user_type)
            except Exception as e:
                print(f"Failed to send user disconnected event to RabbitMQ: {e}")
    except Exception as e:
        print(f"Critical error in WebSocket for chat {chat_id}: {e}")
        connection_info = await chat_manager.disconnect(chat_id, websocket)
        if connection_info:
            try:
                await chat_producer.send_user_disconnected_event(chat_id, connection_info.user_id, connection_info.user_type)
            except Exception as e2:
                print(f"Failed to send user disconnected event to RabbitMQ: {e2}")
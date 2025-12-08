from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, HTTPException
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
    websocket: WebSocket
    user_id: int
    user_type: str
    connected_at: datetime


class ChatConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, List[ChatConnectionInfo]] = {}
    
    async def connect(self, chat_id: int, websocket: WebSocket, user_id: int, user_type: str):
        if chat_id not in self.active_connections:
            self.active_connections[chat_id] = []
        
        connection_info = ChatConnectionInfo(
            websocket=websocket,
            user_id=user_id,
            user_type=user_type,
            connected_at=datetime.utcnow()
        )
        
        self.active_connections[chat_id].append(connection_info)
    
    async def disconnect(self, chat_id: int, websocket: WebSocket) -> Optional[ChatConnectionInfo]:
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
                return connection_info
            else:
                return None
        else:
            return None
    
    def get_connection_info(self, chat_id: int, websocket: WebSocket) -> Optional[ChatConnectionInfo]:
        if chat_id in self.active_connections:
            for conn_info in self.active_connections[chat_id]:
                if conn_info.websocket == websocket:
                    return conn_info
        return None
    
    def get_connected_users(self, chat_id: int) -> List[Dict]:
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
        if chat_id in self.active_connections:
            disconnected_clients = []
            for i, conn_info in enumerate(self.active_connections[chat_id]):
                try:
                    await conn_info.websocket.send_json(message)
                except Exception as e:
                    disconnected_clients.append(i)
            
            for i in reversed(disconnected_clients):
                try:
                    self.active_connections[chat_id].pop(i)
                except Exception:
                    pass

chat_manager = ChatConnectionManager()


@dataclass
class CashboxConnectionInfo:
    websocket: WebSocket
    user_id: int
    cashbox_id: int
    connected_at: datetime


class CashboxConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, List[CashboxConnectionInfo]] = {}
    
    async def connect(self, cashbox_id: int, websocket: WebSocket, user_id: int):
        if cashbox_id not in self.active_connections:
            self.active_connections[cashbox_id] = []
        
        connection_info = CashboxConnectionInfo(
            websocket=websocket,
            user_id=user_id,
            cashbox_id=cashbox_id,
            connected_at=datetime.utcnow()
        )
        
        self.active_connections[cashbox_id].append(connection_info)
    
    async def disconnect(self, cashbox_id: int, websocket: WebSocket) -> Optional[CashboxConnectionInfo]:
        if cashbox_id in self.active_connections:
            connection_info = None
            for conn_info in self.active_connections[cashbox_id]:
                if conn_info.websocket == websocket:
                    connection_info = conn_info
                    self.active_connections[cashbox_id].remove(conn_info)
                    break
            
            if not self.active_connections[cashbox_id]:
                del self.active_connections[cashbox_id]
            
            return connection_info
        return None
    
    async def broadcast_to_cashbox(self, cashbox_id: int, message: dict):
        if cashbox_id in self.active_connections:
            disconnected_clients = []
            for i, conn_info in enumerate(self.active_connections[cashbox_id]):
                try:
                    await conn_info.websocket.send_json(message)
                except Exception as e:
                    disconnected_clients.append(i)
            
            for i in reversed(disconnected_clients):
                try:
                    self.active_connections[cashbox_id].pop(i)
                except Exception:
                    pass

cashbox_manager = CashboxConnectionManager()

@router.websocket("/ws/all/")
async def websocket_all_chats(websocket: WebSocket, token: str = Query(...)):
    try:
        await websocket.accept()
    except Exception as e:
        import traceback
        traceback.print_exc()
        return
    
    cashbox_id = None
    try:
        try:
            user = await get_current_user(token)
        except HTTPException as e:
            error_detail = e.detail if hasattr(e, 'detail') else str(e)
            try:
                await websocket.send_json({
                    "error": "Unauthorized",
                    "detail": error_detail,
                    "status_code": e.status_code
                })
                await websocket.close(code=1008)
            except Exception:
                pass
            return
        except Exception as e:
            import traceback
            traceback.print_exc()
            try:
                await websocket.send_json({"error": "Unauthorized", "detail": str(e)})
                await websocket.close(code=1008)
            except Exception:
                pass
            return
        
        cashbox_id = user.cashbox_id
        await cashbox_manager.connect(cashbox_id, websocket, user.user)
        
        try:
            await websocket.send_json({
                "type": "connected",
                "cashbox_id": cashbox_id,
                "user_id": user.user,
                "message": "Successfully connected to all chats",
                "timestamp": datetime.utcnow().isoformat()
            })
        except Exception as e:
            import traceback
            traceback.print_exc()
            
        while True:
            try:
                data = await websocket.receive_text()
                message_data = json.loads(data)
            except WebSocketDisconnect:
                raise
            except json.JSONDecodeError:
                continue
            except Exception as e:
                import traceback
                traceback.print_exc()
                continue
    
    except WebSocketDisconnect:
        if cashbox_id is not None:
            try:
                await cashbox_manager.disconnect(cashbox_id, websocket)
            except Exception:
                pass
    except Exception as e:
        import traceback
        traceback.print_exc()
        if cashbox_id is not None:
            try:
                await cashbox_manager.disconnect(cashbox_id, websocket)
            except Exception:
                pass

@router.websocket("/ws/{chat_id}/")
async def websocket_chat(chat_id: int, websocket: WebSocket, token: str = Query(...)):
    """WebSocket для чатов с аутентификацией и RabbitMQ"""
    await websocket.accept()
    
    try:
        try:
            user = await get_current_user(token)
        except HTTPException as e:
            error_detail = e.detail if hasattr(e, 'detail') else str(e)
            await websocket.send_json({
                "error": "Unauthorized",
                "detail": error_detail,
                "status_code": e.status_code
            })
            await websocket.close(code=1008)
            return
        except Exception as e:
            await websocket.send_json({"error": "Unauthorized", "detail": str(e)})
            await websocket.close(code=1008)
            import traceback
            traceback.print_exc()
            return
        
        chat = await crud.get_chat(chat_id)
        if not chat:
            await websocket.send_json({"error": "Chat not found", "chat_id": chat_id})
            await websocket.close(code=1008)
            return
        
        chat_cashbox_id = chat.get('cashbox_id') if isinstance(chat, dict) else chat.cashbox_id
        
        if chat_cashbox_id != user.cashbox_id:
            await websocket.send_json({
                "error": "Access denied",
                "detail": "Chat belongs to different cashbox",
                "chat_cashbox_id": chat_cashbox_id,
                "user_cashbox_id": user.cashbox_id
            })
            await websocket.close(code=1008)
            return
        
        user_type = "OPERATOR" if user.is_owner else "OPERATOR"
        
        await chat_manager.connect(chat_id, websocket, user.user, user_type)
        
        try:
            await chat_producer.send_user_connected_event(chat_id, user.user, user_type)
        except Exception as e:
            import traceback
            traceback.print_exc()
        
        try:
            await websocket.send_json({
                "type": "connected",
                "chat_id": chat_id,
                "user_id": user.user,
                "user_type": user_type,
                "message": "Successfully connected to chat",
                "timestamp": datetime.utcnow().isoformat()
            })
        except Exception as e:
            pass
        
        while True:
            try:
                data = await websocket.receive_text()
                message_data = json.loads(data)
                
                event_type = message_data.get("type", "message")
            except WebSocketDisconnect:
                raise
            except json.JSONDecodeError as e:
                await websocket.send_json({
                    "error": "Invalid JSON",
                    "detail": str(e)
                })
                continue
            except Exception as e:
                import traceback
                traceback.print_exc()
                try:
                    await websocket.send_json({
                        "error": "Failed to process message",
                        "detail": str(e)
                    })
                except:
                    pass
                continue
            
            if event_type == "message":
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
                except Exception as e:
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
                    pass
                
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
                is_typing = message_data.get("is_typing", False)
                
                try:
                    await chat_producer.send_typing_event(chat_id, user.user, user_type, is_typing)
                except Exception as e:
                    pass
            
            elif event_type == "get_users":
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
                pass
    except Exception as e:
        import traceback
        traceback.print_exc()
        try:
            connection_info = await chat_manager.disconnect(chat_id, websocket)
            if connection_info:
                try:
                    await chat_producer.send_user_disconnected_event(chat_id, connection_info.user_id, connection_info.user_type)
                except Exception as e2:
                    pass
        except Exception as disconnect_error:
            pass
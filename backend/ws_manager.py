from typing import List, Dict

from fastapi import WebSocket


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[Dict] = []

    async def connect(self, token: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append({"token": token, "socket": websocket})

    async def disconnect(self, token: str, ws: WebSocket):
        # await ws.close()
        self.active_connections.remove({"token": token, "socket": ws})

    async def send_message(self, token: str, message: dict):
        try:
            matches = [
                x for x in self.active_connections if x["token"] == token]
            for ws in matches:
                await ws['socket'].send_json(message)
        except Exception as e:
            print(e)


manager = ConnectionManager()

import json
from ws_manager import manager
from datetime import datetime

async def notify(ws_token: str, event:str, segment_id: int):

    message = {
        "event": event,
        "segment_id": segment_id,
        "timestamp": datetime.now().isoformat()
    }
    await manager.send_message(ws_token, message)
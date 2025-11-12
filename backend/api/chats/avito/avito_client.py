import aiohttp
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from .avito_types import AvitoCredentials
import logging

logger = logging.getLogger(__name__)


class AvitoAPIError(Exception):
    """Avito API error"""
    pass


class AvitoTokenExpiredError(AvitoAPIError):
    """Token expired error - requires refresh"""
    pass


class AvitoClient:
    
    BASE_URL = "https://api.avito.ru"
    MESSENGER_API = f"{BASE_URL}/messenger"
    AUTH_API = f"{BASE_URL}/oauth"
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        token_expires_at: Optional[datetime] = None,
        on_token_refresh: Optional[callable] = None
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.token_expires_at = token_expires_at
        self.on_token_refresh = on_token_refresh
    
    async def _ensure_token_valid(self) -> None:
        if not self.token_expires_at:
            return
        
        if datetime.utcnow() >= self.token_expires_at - timedelta(minutes=5):
            await self.refresh_access_token()
    
    async def refresh_access_token(self) -> Dict[str, Any]:
        if not self.refresh_token:
            raise AvitoTokenExpiredError("No refresh token available")
        
        try:
            async with aiohttp.ClientSession() as session:
                data = {
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "client_id": self.api_key,
                    "client_secret": self.api_secret,
                }
                
                async with session.post(
                    f"{self.AUTH_API}/token",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status != 200:
                        raise AvitoTokenExpiredError(f"Token refresh failed: HTTP {response.status}")
                    
                    result = await response.json()
                    
                    self.access_token = result.get('access_token')
                    self.refresh_token = result.get('refresh_token', self.refresh_token)
                    expires_in = result.get('expires_in', 3600)
                    self.token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                    
                    logger.info("Access token refreshed successfully")
                    
                    if self.on_token_refresh:
                        await self.on_token_refresh({
                            'access_token': self.access_token,
                            'refresh_token': self.refresh_token,
                            'expires_at': self.token_expires_at.isoformat()
                        })
                    
                    return {
                        'access_token': self.access_token,
                        'refresh_token': self.refresh_token,
                        'expires_at': self.token_expires_at.isoformat()
                    }
        
        except aiohttp.ClientError as e:
            raise AvitoTokenExpiredError(f"Token refresh request failed: {str(e)}")
    
    async def get_access_token(self) -> Dict[str, Any]:
        try:
            async with aiohttp.ClientSession() as session:
                data = {
                    "grant_type": "client_credentials",
                    "client_id": self.api_key,
                    "client_secret": self.api_secret,
                }
                
                async with session.post(
                    f"{self.AUTH_API}/token",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Token request failed: HTTP {response.status}, {error_text}")
                        raise AvitoTokenExpiredError(f"Token request failed: HTTP {response.status}")
                    
                    result = await response.json()
                    
                    access_token = result.get('access_token')
                    refresh_token = result.get('refresh_token')
                    expires_in = result.get('expires_in', 3600)
                    expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                    
                    self.access_token = access_token
                    self.refresh_token = refresh_token
                    self.token_expires_at = expires_at
                    
                    logger.info("Initial access token obtained successfully")
                    
                    return {
                        'access_token': access_token,
                        'refresh_token': refresh_token,
                        'expires_at': expires_at.isoformat()
                    }
        
        except aiohttp.ClientError as e:
            logger.error(f"Token request failed: {str(e)}")
            raise AvitoTokenExpiredError(f"Token request failed: {str(e)}")
    
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        base_url: str = None
    ) -> Dict[str, Any]:
        await self._ensure_token_valid()
        
        base = base_url or self.MESSENGER_API
        url = f"{base}{endpoint}"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "X-Developer-Key": self.api_key,
            "Content-Type": "application/json",
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.request(
                    method,
                    url,
                    json=data,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    response_data = await response.json()
                    
                    if response.status == 401:
                        await self.refresh_access_token()
                        return await self._make_request(method, endpoint, data, params, base_url)
                    
                    if response.status >= 400:
                        error_msg = response_data.get('message', f'HTTP {response.status}')
                        logger.error(f"Avito API error {response.status}: {error_msg}")
                        raise AvitoAPIError(f"Avito API error: {error_msg} (HTTP {response.status})")
                    
                    return response_data
                    
            except aiohttp.ClientError as e:
                logger.error(f"Avito API request failed: {str(e)}")
                raise AvitoAPIError(f"Request failed: {str(e)}")
    
    async def get_chats(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        limit = min(limit, 100)
        response = await self._make_request(
            "GET",
            "/v2/user/chats",
            params={"limit": limit, "offset": offset}
        )
        return response.get('chats', [])
    
    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        response = await self._make_request("GET", f"/v2/user/chats/{chat_id}")
        return response.get('chat', {})
    
    async def get_messages(
        self,
        chat_id: str,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        limit = min(limit, 100)
        response = await self._make_request(
            "GET",
            f"/v2/user/chats/{chat_id}/messages",
            params={"limit": limit, "offset": offset}
        )
        return response.get('messages', [])
    
    async def send_message(
        self,
        chat_id: str,
        text: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        if not text and not attachments:
            raise AvitoAPIError("Either text or attachments must be provided")
        
        payload: Dict[str, Any] = {}
        
        if text:
            payload["text"] = text
        
        if attachments:
            payload["attachments"] = attachments
        
        response = await self._make_request(
            "POST",
            f"/v2/user/chats/{chat_id}/messages",
            data=payload
        )
        return response.get('message', {})
    
    async def mark_as_read(self, chat_id: str, message_id: str) -> bool:
        try:
            await self._make_request(
                "POST",
                f"/v2/user/chats/{chat_id}/messages/{message_id}/read"
            )
            logger.info(f"Message {message_id} marked as read")
            return True
        except AvitoAPIError as e:
            logger.warning(f"Failed to mark message as read: {e}")
            return False
    
    async def close_chat(self, chat_id: str) -> bool:
        try:
            await self._make_request(
                "POST",
                f"/v2/user/chats/{chat_id}/close"
            )
            logger.info(f"Chat {chat_id} closed")
            return True
        except AvitoAPIError as e:
            logger.warning(f"Failed to close chat: {e}")
            return False
    
    async def get_user_profile(self) -> Dict[str, Any]:
        response = await self._make_request("GET", "/v2/user")
        return response.get('user', {})
    
    async def validate_token(self) -> bool:
        try:
            await self.get_user_profile()
            return True
        except AvitoAPIError as e:
            logger.error(f"Token validation failed: {e}")
            return False
    
    async def sync_messages(
        self,
        chat_id: str,
        since_timestamp: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        all_messages = []
        offset = 0
        limit = 100
        
        while True:
            messages = await self.get_messages(chat_id, limit=limit, offset=offset)
            
            if not messages:
                break
            
            if since_timestamp:
                filtered = [
                    m for m in messages 
                    if m.get('created_at', 0) > since_timestamp
                ]
                all_messages.extend(filtered)
                if len(filtered) < len(messages):
                    break
            else:
                all_messages.extend(messages)
            
            offset += limit
        
        logger.info(f"Synced {len(all_messages)} messages from chat {chat_id}")
        return all_messages

import aiohttp
import json
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
    AUTH_API = f"{BASE_URL}"  
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        token_expires_at: Optional[datetime] = None,
        on_token_refresh: Optional[callable] = None,
        user_id: Optional[int] = None
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.token_expires_at = token_expires_at
        self.on_token_refresh = on_token_refresh
        self._user_id = user_id  # user_id для эндпоинтов
    
    async def _ensure_token_valid(self) -> None:
        if not self.token_expires_at:
            # Если токен еще не был получен, получаем его
            if not self.access_token:
                await self.get_access_token()
            return
        
        if datetime.utcnow() >= self.token_expires_at - timedelta(minutes=5):
            # Если есть refresh_token, используем его для обновления
            if self.refresh_token:
                await self.refresh_access_token()
            else:
                # Если refresh_token нет, получаем новый токен через client_credentials
                logger.warning("No refresh token available, obtaining new token via client_credentials")
                await self.get_access_token()
    
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
                    f"{self.AUTH_API}/token/",
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
                    "scope": "messenger:read messenger:write"
                }
                
                async with session.post(
                    f"{self.AUTH_API}/token/",
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
    
    @staticmethod
    async def exchange_authorization_code_for_tokens(
        client_id: str,
        client_secret: str,
        authorization_code: str,
        redirect_uri: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            async with aiohttp.ClientSession() as session:
                data = {
                    "grant_type": "authorization_code",
                    "code": authorization_code,
                    "client_id": client_id,
                    "client_secret": client_secret
                }
                
                async with session.post(
                    "https://api.avito.ru/token/",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"OAuth token exchange failed: HTTP {response.status}, {error_text}")
                        raise AvitoAPIError(f"OAuth token exchange failed: HTTP {response.status}")
                    
                    result = await response.json()
                    
                    access_token = result.get('access_token')
                    refresh_token = result.get('refresh_token')
                    expires_in = result.get('expires_in', 3600)
                    expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                    
                    logger.info("OAuth tokens obtained successfully")
                    
                    return {
                        'access_token': access_token,
                        'refresh_token': refresh_token,
                        'expires_at': expires_at.isoformat(),
                        'expires_in': expires_in
                    }
        
        except aiohttp.ClientError as e:
            logger.error(f"OAuth token exchange request failed: {str(e)}")
            raise AvitoAPIError(f"OAuth token exchange request failed: {str(e)}")
    
    async def _get_user_id(self) -> int:
        if self._user_id:
            return self._user_id
        
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                }
                async with session.get(
                    f"{self.BASE_URL}/core/v1/accounts/self",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        user_data = await response.json()
                        user_id = user_data.get('id')
                        if user_id:
                            self._user_id = user_id
                            logger.info(f"Retrieved user_id {user_id} from profile")
                            return user_id
                    
                    logger.warning(f"Failed to get user_id from profile: HTTP {response.status}")
                    raise AvitoAPIError("user_id is required but could not be retrieved from profile")
        except Exception as e:
            logger.error(f"Failed to get user_id: {e}")
            raise AvitoAPIError(f"user_id is required but not set. Error: {str(e)}")
    
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
                    content_type = response.headers.get('Content-Type', '').lower()
                    
                    if 'application/json' in content_type:
                        response_data = await response.json()
                    else:
                        response_text = await response.text()
                        logger.warning(f"Avito API returned non-JSON response (Content-Type: {content_type}): {response_text[:500]}")
                        
                        if response.status < 400:
                            try:
                                response_data = json.loads(response_text) if response_text else {}
                            except:
                                response_data = {"raw_response": response_text}
                        else:
                            response_data = {"message": response_text, "raw_response": response_text}
                    
                    if response.status == 401:
                        await self.refresh_access_token()
                        return await self._make_request(method, endpoint, data, params, base_url)
                    
                    if response.status >= 400:
                        error_msg = response_data.get('message', response_data.get('raw_response', f'HTTP {response.status}'))
                        logger.error(f"Avito API error {response.status}: {error_msg}")
                        raise AvitoAPIError(f"Avito API error: {error_msg} (HTTP {response.status})")
                    
                    return response_data
                    
            except aiohttp.ClientError as e:
                logger.error(f"Avito API request failed: {str(e)}")
                raise AvitoAPIError(f"Request failed: {str(e)}")
    
    async def get_chats(
        self, 
        limit: int = 50, 
        offset: int = 0,
        chat_types: Optional[List[str]] = None,
        unread_only: bool = False,
        item_ids: Optional[List[int]] = None
    ) -> List[Dict[str, Any]]:
        limit = min(limit, 100)
        user_id = await self._get_user_id()
        
        params = {"limit": limit, "offset": offset}
        if chat_types:
            params["chat_types"] = ",".join(chat_types)
        if unread_only:
            params["unread_only"] = "true"
        if item_ids:
            params["item_ids"] = ",".join(map(str, item_ids))
        
        response = await self._make_request(
            "GET",
            f"/v2/accounts/{user_id}/chats",
            params=params
        )
        return response.get('chats', [])
    
    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        user_id = await self._get_user_id()
        response = await self._make_request("GET", f"/v2/accounts/{user_id}/chats/{chat_id}")
        return response
    
    async def get_messages(
        self,
        chat_id: str,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        limit = min(limit, 100)
        user_id = await self._get_user_id()
        response = await self._make_request(
            "GET",
            f"/v3/accounts/{user_id}/chats/{chat_id}/messages/",
            params={"limit": limit, "offset": offset}
        )
        if isinstance(response, list):
            return response
        elif isinstance(response, dict) and 'messages' in response:
            return response.get('messages', [])
        elif isinstance(response, dict):
            logger.warning(f"Unexpected response format from get_messages: {type(response)}")
            return []
        else:
            logger.error(f"Unexpected response type from get_messages: {type(response)}, value: {response}")
            return []
    
    async def upload_image(self, image_data: bytes, filename: str = "image.jpg") -> Optional[tuple]:
        try:
            import aiohttp
            
            max_size = 24 * 1024 * 1024  
            if len(image_data) > max_size:
                raise AvitoAPIError(f"Image size ({len(image_data)} bytes) exceeds maximum allowed size (24 MB)")
            
            user_id = await self._get_user_id()
            endpoint = f"/v1/accounts/{user_id}/uploadImages"
            url = f"{self.MESSENGER_API}{endpoint}"
            
            await self._ensure_token_valid()
            
            from io import BytesIO
            
            form_data = aiohttp.FormData()
            
            content_type = None
            if filename.lower().endswith('.png'):
                content_type = 'image/png'
            elif filename.lower().endswith('.gif'):
                content_type = 'image/gif'
            elif filename.lower().endswith('.webp'):
                content_type = 'image/webp'
            else:
                content_type = 'image/jpeg'
            
            file_obj = BytesIO(image_data)
            file_obj.seek(0)  
            
            form_data.add_field(
                'uploadfile[]',
                value=file_obj,
                filename=filename,
                content_type=content_type
            )
            
            headers = {
                "Authorization": f"Bearer {self.access_token}",
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    data=form_data,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 401:
                        await self.refresh_access_token()
                        headers["Authorization"] = f"Bearer {self.access_token}"
                        async with session.post(
                            url,
                            data=form_data,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=30)
                        ) as retry_response:
                            response = retry_response
                    
                    if response.status >= 400:
                        error_text = await response.text()
                        raise AvitoAPIError(f"Failed to upload image: HTTP {response.status}, {error_text}")
                    
                    response_text = await response.text()
                    
                    try:
                        import json
                        response_data = json.loads(response_text)
                    except json.JSONDecodeError:
                        response_data = response_text
                    
                    if isinstance(response_data, dict):
                        image_id = list(response_data.keys())[0] if response_data else None
                        if image_id:
                            image_urls = response_data[image_id]
                            if isinstance(image_urls, dict):
                                image_url = image_urls.get('1280x960') or image_urls.get('640x480') or (list(image_urls.values())[0] if image_urls else None)
                                return (image_id, image_url)
                            else:
                                return (image_id, None)
                    
                    return None
        except Exception as e:
            raise AvitoAPIError(f"Failed to upload image: {str(e)}")
    
    async def send_message(
        self,
        chat_id: str,
        text: Optional[str] = None,
        image_id: Optional[str] = None
    ) -> Dict[str, Any]:
        if not text and not image_id:
            raise AvitoAPIError("Either text or image_id must be provided")
        
        user_id = await self._get_user_id()
        
        if image_id:
            payload = {
                "image_id": image_id
            }
            response = await self._make_request(
                "POST",
                f"/v1/accounts/{user_id}/chats/{chat_id}/messages/image",
                data=payload
            )
        else:
            payload = {
                "message": {
                    "text": text
                },
                "type": "text"
            }
            response = await self._make_request(
                "POST",
                f"/v1/accounts/{user_id}/chats/{chat_id}/messages",
                data=payload
            )
        
        return response
    
    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        try:
            user_id = await self._get_user_id()
            endpoint = f"/v1/accounts/{user_id}/chats/{chat_id}/messages/{message_id}/delete"
            
            try:
                await self._make_request(
                    "POST",
                    endpoint,
                    base_url=self.MESSENGER_API,
                    data={}  
                )
            except AvitoAPIError as e:
                if "404" in str(e):
                    try:
                        endpoint_without_delete = f"/v1/accounts/{user_id}/chats/{chat_id}/messages/{message_id}"
                        await self._make_request(
                            "DELETE",
                            endpoint_without_delete,
                            base_url=self.MESSENGER_API
                        )
                    except AvitoAPIError:
                        try:
                            await self._make_request(
                                "POST",
                                endpoint_without_delete,
                                base_url=self.MESSENGER_API,
                                data={"action": "delete"}
                            )
                        except AvitoAPIError as e3:
                            raise e3
                else:
                    raise
            
            logger.info(f"Message {message_id} deleted in chat {chat_id}")
            return True
        except AvitoAPIError as e:
            logger.warning(f"Failed to delete message {message_id} in chat {chat_id}: {e}")
            return False
    
    async def mark_chat_as_read(self, chat_id: str) -> bool:
        try:
            user_id = await self._get_user_id()
            await self._make_request(
                "POST",
                f"/v1/accounts/{user_id}/chats/{chat_id}/read"
            )
            logger.info(f"Chat {chat_id} marked as read")
            return True
        except AvitoAPIError as e:
            logger.warning(f"Failed to mark chat as read: {e}")
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
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                }
                async with session.get(
                    f"{self.BASE_URL}/core/v1/accounts/self",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        user_data = await response.json()
                        logger.info(f"Retrieved user profile: {user_data.get('id')}")
                        return user_data
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to get user profile: HTTP {response.status}, {error_text}")
                        raise AvitoAPIError(f"Failed to get user profile: HTTP {response.status}")
        except aiohttp.ClientError as e:
            logger.error(f"Error getting user profile: {str(e)}")
            raise AvitoAPIError(f"Error getting user profile: {str(e)}")
    
    async def validate_token(self) -> bool:
        try:
            await self.get_chats(limit=1)
            return True
        except AvitoAPIError as e:
            logger.error(f"Token validation failed: {e}")
            return False
    
    async def check_status(self) -> Dict[str, Any]:
        """
        Проверяет статус аккаунта Avito и возвращает статус-код и информацию о подключении.
        Возвращает: {'status_code': int, 'connection_status': str, 'success': bool}
        """
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                }
                async with session.get(
                    f"{self.BASE_URL}/core/v1/accounts/self",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    status_code = response.status
                    
                    if status_code == 200:
                        user_data = await response.json()
                        logger.info(f"Status check successful for user {user_data.get('id')}")
                        return {
                            'status_code': status_code,
                            'connection_status': 'connected',
                            'success': True
                        }
                    else:
                        error_text = await response.text()
                        logger.warning(f"Status check failed: HTTP {status_code}, {error_text}")
                        
                        if status_code == 401:
                            connection_status = 'unauthorized'
                        elif status_code == 403:
                            connection_status = 'forbidden'
                        elif status_code == 404:
                            connection_status = 'not_found'
                        else:
                            connection_status = 'error'
                        
                        return {
                            'status_code': status_code,
                            'connection_status': connection_status,
                            'success': False,
                            'error': error_text
                        }
        except aiohttp.ClientError as e:
            logger.error(f"Status check error: {str(e)}")
            return {
                'status_code': 0,
                'connection_status': 'error',
                'success': False,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error during status check: {str(e)}")
            return {
                'status_code': 0,
                'connection_status': 'error',
                'success': False,
                'error': str(e)
            }
    
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
                    if m.get('created', 0) > since_timestamp
                ]
                all_messages.extend(filtered)
                if len(filtered) < len(messages):
                    break
            else:
                all_messages.extend(messages)
            
            offset += limit
            
            if len(messages) < limit:
                break
        
        logger.info(f"Synced {len(all_messages)} messages from chat {chat_id}")
        return all_messages
    
    async def register_webhook(self, webhook_url: str) -> Dict[str, Any]:
        try:
            payload = {
                "url": webhook_url
            }
            
            endpoints_to_try = [
                "/v3/webhook",
                "/v3/subscriptions",
                "/v3/webhooks",
            ]
            
            for endpoint in endpoints_to_try:
                try:
                    response = await self._make_request(
                        "POST",
                        endpoint,
                        data=payload,
                        base_url=self.MESSENGER_API
                    )
                    logger.info(f"✅ Webhook registered successfully at {endpoint}: {webhook_url}")
                    logger.info(f"Response from Avito API: {response}")
                    return response
                except AvitoAPIError as e:
                    if "404" not in str(e) and "not found" not in str(e).lower():
                        raise
                    continue
            
            try:
                response = await self._make_request(
                    "POST",
                    "/v3/subscriptions",
                    data=payload
                )
                logger.info(f"✅ Webhook registered successfully via fallback endpoint: {webhook_url}")
                logger.info(f"Response from Avito API: {response}")
                return response
            except AvitoAPIError as e:
                logger.error(f"Failed to register webhook: {e}")
                raise AvitoAPIError(f"Could not register webhook. Tried multiple endpoints. Last error: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error registering webhook: {e}")
            raise AvitoAPIError(f"Failed to register webhook: {str(e)}")
    
    async def get_webhooks(self) -> List[Dict[str, Any]]:
        try:
            endpoints_to_try = [
                "/v1/subscriptions",
            ]
            
            for endpoint in endpoints_to_try:
                try:
                    response = await self._make_request(
                        "POST",
                        endpoint,
                        base_url=self.MESSENGER_API
                    )
                    if isinstance(response, dict) and 'subscriptions' in response:
                        subscriptions = response.get('subscriptions', [])
                        logger.info(f"Got {len(subscriptions)} subscriptions from {endpoint}")
                        return subscriptions
                    elif isinstance(response, list):
                        logger.info(f"Got {len(response)} subscriptions as list from {endpoint}")
                        return response
                    elif isinstance(response, dict) and 'webhooks' in response:
                        webhooks = response.get('webhooks', [])
                        logger.info(f"Got {len(webhooks)} webhooks from {endpoint}")
                        return webhooks
                    elif isinstance(response, dict):
                        logger.warning(f"Unexpected response format from {endpoint}: {response}")
                        return [response]
                    logger.warning(f"Empty response from {endpoint}")
                    return []
                except AvitoAPIError as e:
                    error_str = str(e)
                    if "402" in error_str or "подписку" in error_str.lower():
                        logger.warning(f"Subscription required (402) for getting webhooks from {endpoint}: {e}")
                    elif "404" not in error_str and "not found" not in error_str.lower():
                        logger.warning(f"Error getting webhooks from {endpoint}: {e}")
                    continue
            
            logger.warning("Could not get webhooks list - all endpoints returned 404 or error")
            return []
        
        except Exception as e:
            logger.error(f"Error getting webhooks: {e}")
            return []
    
    async def get_voice_file_url(self, voice_id: str) -> Optional[str]:
        try:
            user_id = await self._get_user_id()
            endpoint = f"/v1/accounts/{user_id}/getVoiceFiles"
            
            params = {"voice_ids": [voice_id]}
            response = await self._make_request("GET", endpoint, params=params, base_url=self.MESSENGER_API)
            
            voices_urls = response.get('voices_urls', {})
            voice_url = voices_urls.get(voice_id)
            
            if voice_url:
                voice_url = voice_url.replace('\u0026', '&')
                return voice_url
            
            return None
        except Exception as e:
            logger.warning(f"Failed to get voice file URL for voice_id {voice_id}: {e}")
            return None
    
    async def unsubscribe_webhook(self, webhook_url: str) -> Dict[str, Any]:
        try:
            payload = {
                "url": webhook_url
            }
            
            response = await self._make_request(
                "POST",
                "/v1/webhook/unsubscribe",
                data=payload,
                base_url=self.MESSENGER_API
            )
            
            logger.info(f"✅ Webhook unsubscribed successfully: {webhook_url}")
            return response
        
        except Exception as e:
            logger.error(f"Error unsubscribing webhook: {e}")
            raise AvitoAPIError(f"Failed to unsubscribe webhook: {str(e)}")

import aiohttp
import asyncio
from typing import Union

from apps.geocoders.core.base_instance import BaseGeocoder
from apps.geocoders.utils import AsyncLRU


class Geoapify(BaseGeocoder):
    _instance = None
    _session: Union[aiohttp.ClientSession, None] = None
    _lock = asyncio.Lock()

    def __new__(cls, api_key: Union[str, None] = None):
        if cls._instance is None:
            if not api_key:
                raise ValueError("Geoapify API key is required")
            cls._instance = super().__new__(cls)
            cls._instance.api_key = api_key
            cls._instance.autocomplete_url = "https://api.geoapify.com/v1/geocode/autocomplete"
            cls._instance.search_url = "https://api.geoapify.com/v1/geocode/search"
            cls._instance.autocomplete_cache = AsyncLRU()
            cls._instance.search_cache = AsyncLRU()
        return cls._instance

    async def _get_session(self):
        async with self._lock:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    async def autocomplete(self, text: str, limit=5) -> Union[list[str], list]:
        return await self.autocomplete_cache.get(key=text, func=self._autocomplete, text=text, limit=limit)

    async def _autocomplete(self, text, limit=5) -> Union[list[str], list]:
        try:
            session = await self._get_session()
            params = {"text": text, "apiKey": self.api_key, "limit": limit, "lang": "ru"}
            async with session.get(self.autocomplete_url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return [f["properties"]["formatted"].split(",")[0] for f in data.get("features", [])]
        except aiohttp.ClientError as e:
            return []

    async def validate_address(self, address: str, limit=1) -> bool:
        return await self.search_cache.get(address, func=self._validate_address, address=address, limit=limit)

    async def _validate_address(self, address: str, limit=1) -> bool:
        try:
            session = await self._get_session()
            params = {"text": address, "apiKey": self.api_key, "limit": limit, "lang": "ru"}
            async with session.get(self.search_url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                features = data.get("features")
                if features:
                    return True if features[0]["properties"] else False
                else:
                    return False
        except aiohttp.ClientError as e:
            return False

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None


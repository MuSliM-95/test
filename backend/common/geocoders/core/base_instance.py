from common.geocoders.schemas import GeocoderSearchResponse
from abc import ABC, abstractmethod
from typing import Union, List

class BaseGeocoder(ABC):
    @abstractmethod
    async def autocomplete(self, text: str, limit=5) -> Union[List[str], List]:
        pass

    @abstractmethod
    async def validate_address(self, address: str, limit=1) -> Union[GeocoderSearchResponse, None]:
        pass

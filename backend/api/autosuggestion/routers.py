from fastapi import APIRouter

from api.autosuggestion.schemas import AutosuggestResponse
from common.geocoders.instance import geocoder

router = APIRouter(prefix="/autosuggestions", tags=["autosuggestions"])


@router.get("/geolocation", response_model=AutosuggestResponse)
async def autosuggest_location(query: str, limit: int = 5):
    suggestions = await geocoder.autocomplete(query, limit=limit)
    return AutosuggestResponse(suggestions=suggestions)

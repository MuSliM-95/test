from pydantic import BaseModel

from apps.booking.events.domain.models.BaseSearchEventFiltersModel import BaseSearchEventFiltersModel


class SearchEventFiltersModel(BaseModel):
    filters: BaseSearchEventFiltersModel
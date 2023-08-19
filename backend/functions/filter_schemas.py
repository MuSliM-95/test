from pydantic import BaseModel
from typing import Optional

class PaymentFiltersQuery(BaseModel):
    name: Optional[str]
    tags: Optional[str]
    external_id: Optional[str]
    relship: Optional[str]
    project: Optional[str]
    contragent: Optional[str]
    paybox: Optional[str]
    paybox_to: Optional[str]
    dateto: Optional[str]
    datefrom: Optional[str]
    payment_type: Optional[str]

class AnalyticsFiltersQuery(BaseModel):
    datefrom: Optional[int]
    dateto: Optional[int]
    paybox_id: Optional[str]
    status: Optional[str]

class ChequesFiltersQuery(BaseModel):
    datefrom: Optional[int]
    dateto: Optional[int]
    user: Optional[int]

class PayboxesFiltersQuery(BaseModel):
    external_id: Optional[str]
    name: Optional[str]

class ProjectsFiltersQuery(BaseModel):
    external_id: Optional[str]
    name: Optional[str]

class ArticlesFiltersQuery(BaseModel):
    name: Optional[str]

class UsersFiltersQuery(BaseModel):
    external_id: Optional[str]

class CAFiltersQuery(BaseModel):
    name: Optional[str]
    inn: Optional[int]
    phone: Optional[int]
    external_id: Optional[str]

class PicturesFiltersQuery(BaseModel):
    entity: Optional[str]
    entity_id: Optional[int]


class PricesFiltersQuery(BaseModel):
    name: Optional[str]
    type: Optional[str]
    description_short: Optional[str]
    description_long: Optional[str]
    code: Optional[int]
    unit: Optional[int]
    category: Optional[int]
    manufacturer: Optional[int]
    price_type_id: Optional[int]
    date_from: Optional[int]
    date_to: Optional[int]

import re
from typing import Optional, Dict, List, Any

from fastapi import HTTPException
from pydantic import BaseModel, constr, Field, validator

XML_TAG_RE = re.compile(r'^[A-Za-z_:][A-Za-z0-9._:\-]*$')

XML_TAG = constr(regex=r'^[A-Za-z_:][A-Za-z0-9._:\-]*$')

ALLOWED_DB_FIELDS = ['name', 'description', 'category', 'price',
                     'current_amount', 'images', 'params']



def is_valid_xml_tag(tag: str) -> bool:
    if not isinstance(tag, str) or not tag:
        return False
    if tag.lower().startswith("xml"):
        return False
    return bool(XML_TAG_RE.match(tag))


class PricesFeed(BaseModel):
    from_: float = Field(..., alias='from')
    to_: float = Field(..., alias='to')


class CriteriaFeed(BaseModel):
    warehouse_id: Optional[List[int]]
    only_on_stock: Optional[bool]
    prices: Optional[PricesFeed]
    category_id: Optional[List[int]]
    price_types_id: Optional[int]


class FeedCreate(BaseModel):
    name: str
    description: Optional[str]
    root_tag: XML_TAG
    item_tag: XML_TAG
    field_tags: Dict[str, str]
    criteria: Optional[CriteriaFeed]

    @validator("field_tags")
    def validate_field_tags(cls, values: Dict[str, str]) -> Dict[str, str]:
        for xml_tag, value in values.items():
            if not is_valid_xml_tag(xml_tag):
                raise HTTPException(status_code=400,
                                    detail=f"Invalid xml tag: {xml_tag}")
            if value not in ALLOWED_DB_FIELDS:
                raise HTTPException(status_code=400,
                                    detail=f"Field not allowed: {value}")
        return values


class Feed(BaseModel):
    id: int
    name: str
    description: Optional[str]
    root_tag: str
    item_tag: str
    field_tags: Dict[str, str]
    criteria: Optional[Dict[str, Any]]
    url_token: str


class GetFeeds(BaseModel):
    count: int
    feeds: List[Feed]


class FeedUpdate(FeedCreate):
    name: Optional[str]
    root_tag: Optional[XML_TAG]
    item_tag: Optional[XML_TAG]
    field_tags: Optional[Dict[str, str]]

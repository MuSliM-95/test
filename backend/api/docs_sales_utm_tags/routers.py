from fastapi import APIRouter, HTTPException, Depends

from api.docs_sales_utm_tags import schemas
from api.docs_sales_utm_tags.service import DocsSalesUTMTagsService
from database.db import cboxes, database, docs_sales, docs_sales_utm_tags
from functions.helpers import get_user_by_token, check_entity_exists
from sqlalchemy import select, and_

router = APIRouter(tags=["docs_sales"])


@router.post("/docs_sales/{idx}/utm", response_model=schemas.UtmTag)
async def create_utm_tag(token: str, idx: int, utm_tags_data: schemas.CreateUTMTag, service: DocsSalesUTMTagsService = Depends()):
    return await service.create_utm_tag(token, idx, utm_tags_data)

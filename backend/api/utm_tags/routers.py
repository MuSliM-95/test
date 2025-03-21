from fastapi import APIRouter, HTTPException

from api.utm_tags import schemas
from database.db import database, docs_sales, utm_tags
from functions.helpers import get_user_by_token, check_entity_exists


router = APIRouter(prefix="/utm", tags=["utm_tags"])


@router.post("/", response_model=schemas.UtmTag)
async def create_utm_tag(token: str, utm_tags_data: schemas.CreateUTMTag):
    await get_user_by_token(token)
    data = utm_tags_data.dict()
    await check_entity_exists(docs_sales, data["docs_sales_id"])
    check_utm_exist_query = utm_tags.select().where(utm_tags.c.docs_sales_id == data["docs_sales_id"])
    if await database.execute(check_utm_exist_query):
        raise HTTPException(400, detail="This document already exists")
    query = utm_tags.insert().values(data)
    new_utm_id = await database.execute(query)
    query = utm_tags.select().where(utm_tags.c.id == new_utm_id)
    utm = await database.fetch_one(query)
    return utm

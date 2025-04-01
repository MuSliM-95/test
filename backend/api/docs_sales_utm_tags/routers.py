from fastapi import APIRouter, HTTPException

from api.docs_sales_utm_tags import schemas
from database.db import cboxes, database, docs_sales, docs_sales_utm_tags
from functions.helpers import get_user_by_token, check_entity_exists
from sqlalchemy import select, and_

router = APIRouter(tags=["docs_sales"])


@router.post("/docs_sales/{idx}/utm", response_model=schemas.UtmTag)
async def create_utm_tag(token: str, idx: int, utm_tags_data: schemas.CreateUTMTag):
    user = await get_user_by_token(token)

    check_docs_sales_exist = (
        select(docs_sales.c.id)
        .where(and_(
            docs_sales.c.id == idx,
            docs_sales.c.cashbox == user.cashbox_id,
            docs_sales.c.is_deleted == False
        ))
    )

    if not await database.fetch_one(check_docs_sales_exist):
        raise HTTPException(
            status_code=404,
            detail="docs_sale не существует!",
        )

    check_utm_exist_query = select(docs_sales_utm_tags.c.id).where(docs_sales_utm_tags.c.docs_sales_id == idx)
    if await database.fetch_one(check_utm_exist_query):
        raise HTTPException(400, detail="This document already exists")

    data = utm_tags_data.dict()

    query = docs_sales_utm_tags.insert().values(
        docs_sales_id=idx,
        utm_source=utm_tags_data.utm_source,
        utm_medium=utm_tags_data.utm_medium,
        utm_campaign=utm_tags_data.utm_campaign,
        utm_term=utm_tags_data.utm_term,
        utm_content=utm_tags_data.utm_content,
        utm_name=utm_tags_data.utm_name,
        utm_phone=utm_tags_data.utm_phone,
        utm_email=utm_tags_data.utm_email,
        utm_leadid=utm_tags_data.utm_leadid,
        utm_yclientid=utm_tags_data.utm_yclientid,
        utm_gaclientid=utm_tags_data.utm_gaclientid,
    )
    new_utm_id = await database.execute(query)

    return schemas.UtmTag(
        id=new_utm_id,
        docs_sales_id=idx,
        **data
    )

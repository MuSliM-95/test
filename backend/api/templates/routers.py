from fastapi import APIRouter, HTTPException, UploadFile, File
from database.db import database, doc_templates, entity_to_entity, pages, areas
import api.templates.schemas as schemas
from datetime import datetime
from sqlalchemy import or_, select
from typing import Dict, Union

from functions.helpers import get_user_by_token


router = APIRouter(tags=["doctemplates"])


@router.get("/doctemplates/")
async def get_list_template(token: str, tags: str = None, limit: int = 100, offset: int = 0, page: str = None, area: str = None):
    """Получение списка шаблонов документов"""
    user = await get_user_by_token(token)
    filter_tags = []
    _filter = []
    if page:
        query_pages = \
            select(
                entity_to_entity.c.from_id).\
            join(
                pages, pages.c.id == entity_to_entity.c.to_id).\
            where(
                pages.c.name == page,
                entity_to_entity.c.cashbox_id == user.cashbox_id,
                or_(entity_to_entity.c.to_entity == 5)).\
            subquery('query_pages')
        _filter.append(doc_templates.c.id.in_(query_pages))

    if area:
        query_areas = \
            select(
                entity_to_entity.c.from_id).\
            join(
                areas, areas.c.id == entity_to_entity.c.to_id).\
            where(
                areas.c.name == area,
                entity_to_entity.c.cashbox_id == user.cashbox_id,
                or_(entity_to_entity.c.to_entity == 4)).\
            subquery('query_areas')
        _filter.append(doc_templates.c.id.in_(query_areas))

    if tags:
        tags = list(map(lambda x: x.strip().lower(), tags.replace(' ', '').strip().split(',')))
        filter_tags = list(map(lambda x: doc_templates.c.tags.like(f'%{x}%'), tags))

    query = \
        select(
            doc_templates
        ).\
        where(
            doc_templates.c.cashbox == user.cashbox_id,
            *_filter,
            *filter_tags
        )
    result = await database.fetch_all(query)
    return {'result': result, 'tags': ','.join(tags) if tags else ''}


@router.get("/doctemplates/{idx}/", response_model=schemas.DocTemplate)
async def get_template(token: str, idx: int):

    """Получение шаблона по ID"""

    user = await get_user_by_token(token)
    query = doc_templates.select().where(doc_templates.c.id == idx, doc_templates.c.cashbox == user.cashbox_id)
    result = await database.fetch_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"У вас нет шаблона с таким id")
    return result


@database.transaction()
@router.post("/doctemplates/", response_model=schemas.DocTemplateCreate)
async def add_template(token: str, name: str, areas_in: list = None, pages_in: list = None,  description: str = None, tags: str = None, doc_type: int = None, file: Union[UploadFile, None] = None):

    """Добавление нового шаблона"""

    try:
        user = await get_user_by_token(token)
        template_res = dict({
            'name': name,
            'cashbox': user.cashbox_id,
            'description': description,
            'user_id': user.id,
            'type': doc_type,
            'tags': ','.join(
                sorted(
                    list(
                        map(
                            lambda x:
                            x.strip(), tags.strip().split(','))), key=str.lower))
            if tags else None,
            'template_data': str(file.file.read().decode('UTF-8')) if file else None,
            'is_deleted': False
        })

        template_res["created_at"] = int(datetime.utcnow().timestamp())
        template_res["updated_at"] = int(datetime.utcnow().timestamp())
        query = doc_templates.insert().values(template_res)
        result_id = await database.execute(query)
        query = doc_templates.select().where(doc_templates.c.id == result_id)
        result = await database.fetch_one(query)

        if areas_in[0] != '':
            await database.execute_many( entity_to_entity.insert(), values=
                [
                        {
                            "from_entity": 3,
                            "to_entity": 4,
                            "from_id": result['id'],
                            "to_id": int(item),
                            "status": True,
                            "delinked": False,
                            "cashbox_id": user.cashbox_id,
                            "type": "docs_template_areas"
                        }
                    for item in areas_in[0].split(",") if int(item) > 0
                ]
            )

        if pages_in[0] != '':
            await database.execute_many(entity_to_entity.insert(),values=
                [
                    {
                            "from_entity": 3,
                            "to_entity": 5,
                            "from_id": result['id'],
                            "to_id": int(item),
                            "status": True,
                            "delinked": False,
                            "cashbox_id": user.cashbox_id,
                            "type": "docs_template_pages"
                    }
                for item in pages_in[0].split(",") if int(item) > 0
                ]
            )
        return result
    except Exception as error:
        raise HTTPException(status_code=433, detail=str(error))


@router.delete("/doctemplates/{idx}/", response_model=schemas.DocTemplate)
async def delete_template(token: str, idx: int):
    """Удаление шаблона по ID"""
    user = await get_user_by_token(token)
    query = \
        doc_templates.\
            update().\
            where(doc_templates.c.id == idx, doc_templates.c.cashbox == user.cashbox_id).\
            values(dict(is_deleted=True, updated_at=int(datetime.utcnow().timestamp())))
    result = await database.execute(query)
    query = doc_templates.select().where(doc_templates.c.id == idx, doc_templates.c.cashbox == user.cashbox_id)
    result = await database.fetch_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"У вас нет шаблона с таким id")
    return result


@router.patch("/doctemplates/{idx}/", response_model=schemas.DocTemplateUpdate)
async def update_template(token: str, idx: int, template: Dict):
    """Обновление шаблона по ID"""
    user = await get_user_by_token(token)
    template['updated_at'] = int(datetime.utcnow().timestamp())
    query = doc_templates.\
        update().\
        where(doc_templates.c.id == idx, doc_templates.c.cashbox == user.cashbox_id).values(template)
    await database.execute(query)
    query = doc_templates.select().where(doc_templates.c.id == idx, doc_templates.c.cashbox == user.cashbox_id)
    result = await database.fetch_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"У вас нет шаблона с таким id")
    return result

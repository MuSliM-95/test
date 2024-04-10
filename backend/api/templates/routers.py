from fastapi import APIRouter, HTTPException, UploadFile, File
from database.db import database, doc_templates, entity_to_entity, pages, areas
import api.templates.schemas as schemas
from datetime import datetime
from sqlalchemy import or_, select
from typing import Dict, Union, List

from functions.helpers import get_user_by_token


router = APIRouter(tags=["doctemplates"])


@router.get("/doctemplates/", response_model=schemas.TemplateList)
async def get_list_template(token: str, tags: str = None, limit: int = 100, offset: int = 0, page: str = None, area: str = None):
    """Получение списка шаблонов документов"""
    user = await get_user_by_token(token)
    if tags:
        tags = list(map(lambda x: x.strip().lower(), tags.replace(' ', '').strip().split(',')))
        filter_tags = list(map(lambda x: doc_templates.c.tags.like(f'%{x}%'), tags))
        query = (select().
                 where(or_(*filter_tags),
                       doc_templates.c.cashbox == user.cashbox_id,
                       entity_to_entity.c.from_entity == 10,
                       or_(entity_to_entity.c.to_entity == 12, entity_to_entity.c.to_entity == 13),
                       pages.c.name.like(f'%{page}%'),
                       areas.c.name.like(f'%{area}%'),
                       ).
                 join(entity_to_entity, entity_to_entity.c.from_id == doc_templates.c.id).
                 join(pages, entity_to_entity.c.to_id == pages.c.id).
                 join(areas, entity_to_entity.c.to_id == areas.c.id).
                 limit(limit).
                 offset(offset))
        result = await database.fetch_all(query)
        return {'result': result, 'tags': ','.join(tags)}
    else:
        query = select().where(doc_templates.c.cashbox == user.cashbox_id).limit(limit).offset(offset)
        result = await database.fetch_all(query)
        return {'result': result, 'tags': ''}


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
async def add_template(token: str, name: str, areas_in: List[Union[int, None]] = None, pages_in: List[Union[int, None]] = None,  description: str = None, tags: str = None, doc_type: int = None, file: Union[UploadFile, None] = None):

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
        await database.execute_many( entity_to_entity.insert(), values=
            [
                    {
                        "from_entity": 10,
                        "to_entity": 12,
                        "from_id": result['id'],
                        "to_id": item,
                        "status": True,
                        "delinked": False,
                        "cashbox_id": user.cashbox_id,
                        "type": "docs_template_pages"
                    }
                for item in areas_in
            ]
        )
        await database.execute_many(entity_to_entity.insert(),values=
            [
                {
                        "from_entity": 10,
                        "to_entity": 13,
                        "from_id": result['id'],
                        "to_id": item,
                        "status": True,
                        "delinked": False,
                        "cashbox_id": user.cashbox_id,
                        "type": "docs_template_pages"
                }
            for item in pages_in
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

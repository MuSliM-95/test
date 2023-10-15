from fastapi import APIRouter, HTTPException, UploadFile, File
from database.db import database, doc_templates
import api.templates.schemas as schemas
from datetime import datetime
from sqlalchemy import or_
from typing import Dict

from functions.helpers import get_user_by_token


router = APIRouter(tags=["doctemplates"])


@router.get("/doctemplates/", response_model=schemas.TemplateList)
async def get_list_template(token: str, tags: str = None, limit: int = 100, offset: int = 0):
    """Получение списка шаблонов документов"""
    user = await get_user_by_token(token)
    if tags:
        tags = list(map(lambda x: x.strip().lower(), tags.replace(' ', '').strip().split(',')))
        filter_tags = list(map(lambda x: doc_templates.c.tags.like(f'%{x}%'), tags))
        query = doc_templates.select().where(or_(*filter_tags)).limit(limit).offset(offset)
        result = await database.fetch_all(query)
        return {'result': result, 'tags': ','.join(tags)}
    else:
        query = doc_templates.select().limit(limit).offset(offset)
        result = await database.fetch_all(query)
        return {'result': result, 'tags': ''}


@router.get("/doctemplates/{idx}/", response_model=schemas.DocTemplate)
async def get_template(token: str, idx: int):
    """Получение шаблона по ID"""
    user = await get_user_by_token(token)
    query = doc_templates.select().where(doc_templates.c.id == idx)
    result = await database.fetch_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"У вас нет шаблона с таким id")
    return result


@router.post("/doctemplates/", response_model=schemas.DocTemplateCreate)
async def add_template(token: str, name: str, description: str, tags: str, doc_type: int, file: UploadFile = File(None)):
    """Добавление нового шаблона"""
    try:
        user = await get_user_by_token(token)
        template_res = dict({
            'name': name,
            'description': description,
            'user_id': user.id,
            'type': doc_type,
            'tags': ','.join(sorted(list(map(lambda x: x.strip(), tags.strip().split(','))), key=str.lower)),
            'template_data': str(file.file.read().decode('UTF-8')),
            'is_deleted': False
        })
        template_res["created_at"] = int(datetime.utcnow().timestamp())
        template_res["updated_at"] = int(datetime.utcnow().timestamp())
        query = doc_templates.insert().values(template_res)
        result_id = await database.execute(query)
        query = doc_templates.select().where(doc_templates.c.id == result_id)
        result = await database.fetch_one(query)
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
            where(doc_templates.c.id == idx).\
            values(dict(is_deleted=True, updated_at=int(datetime.utcnow().timestamp())))
    result = await database.execute(query)
    query = doc_templates.select().where(doc_templates.c.id == idx)
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
        where(doc_templates.c.id == idx).values(template)
    result = await database.execute(query)
    query = doc_templates.select().where(doc_templates.c.id == idx)
    result = await database.fetch_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"У вас нет шаблона с таким id")
    return result

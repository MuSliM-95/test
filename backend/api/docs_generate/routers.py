import datetime
import pdfkit

from fastapi import APIRouter, HTTPException, status
from uuid import uuid4
from fastapi.responses import Response, FileResponse
from jinja2 import Template
from jinja2.filters import FILTERS
from typing import Dict, Any
from io import BytesIO
import qrcode
from PIL import Image

from api.docs_generate.schemas import TypeDoc
from database.db import database, doc_generated, doc_templates
from functions.helpers import get_user_by_token
import base64
from sqlalchemy import or_, desc
from os import environ

import aioboto3


def render_qrcode(value):
    qr_image = qrcode.make(value, box_size=15)
    qr_image_pil = qr_image.get_image()
    stream = BytesIO()
    qr_image_pil.save(stream, format='PNG')
    qr_image_data = stream.getvalue()
    qr_image_base64 = base64.b64encode(qr_image_data).decode('utf-8')
    return f"data:image/png;base64,{qr_image_base64}"


FILTERS['render_qrcode'] = render_qrcode

s3_session = aioboto3.Session()

s3_data = {
    "service_name": "s3",
    "endpoint_url": "https://storage.clo.ru",
    "aws_access_key_id": environ.get("S3_ACCESS"),
    "aws_secret_access_key": environ.get("S3_SECRET"),
}

# bucket_name = "s3testbucket-default-bucket"
bucket_name = "apptablecrmcom-default-bucket"

router = APIRouter(tags=["docgenerated"])


def generate_doc(template, *kwargs) -> Any:
    try:
        tm = Template(template)
        return tm.render(*kwargs)
    except Exception as error:
        return error


def convert_html_to_pdf(doc_render):
    return pdfkit.from_string(doc_render)


@router.post('/docgenerated/')
async def doc_generate(token: str,
                       template_id: int,
                       variable: Dict,
                       entity: str,
                       entity_id: int,
                       type_doc: TypeDoc,
                       tags: str = None):
    '''Генерирование документа с загрузкой в S3 и фиксацией записи генерации'''
    try:
        user = await get_user_by_token(token)
        query = doc_templates.select().where(doc_templates.c.id == template_id)
        template = await database.fetch_one(query)
        data = generate_doc(template['template_data'], variable)

        file_link = f"docsgenerate/{entity}_{entity_id}_{uuid4().hex[:8]}"
        if type_doc is TypeDoc.html:
            file_link += ".html"
        if type_doc is TypeDoc.pdf:
            data = convert_html_to_pdf(data)
            file_link += ".pdf"

        async with s3_session.client(**s3_data) as s3:
            await s3.put_object(Body=data, Bucket=bucket_name, Key=file_link)

            if type_doc is TypeDoc.pdf:
                await s3.put_object_acl(
                    Bucket=bucket_name,
                    Key=file_link,
                    ACL='public-read'
                )

        file_dict = {
            'cashbox_id': user.cashbox_id,
            'doc_link': file_link,
            'created_at': datetime.datetime.now(),
            'tags': tags.lower(),
            'template_id': template_id,
            'entity': entity,
            'entity_id': entity_id,
            'type_doc': type_doc
        }
        query = doc_generated.insert().values(file_dict)
        result_file_dict_id = await database.execute(query)
        query = doc_generated.select().where(doc_generated.c.id == result_file_dict_id)
        result = await database.fetch_one(query)
        return result
    except Exception as error:
        raise HTTPException(status_code=423, detail=str(error))


@router.get('/docgenerated/{idx}', status_code=status.HTTP_200_OK)
async def get_doc_generate_by_idx(token: str, idx: int):
    """Получение реквизитов документа по ID"""
    user = await get_user_by_token(token)
    query = doc_generated.select().where(doc_generated.c.id == idx)
    result = await database.fetch_one(query)
    return result


@router.get("/docgenerated/file/{filename}/")
async def get_generate_docs_by_filename(filename: str, type_doc: TypeDoc):
    """Получение документа по имени файла"""
    async with s3_session.client(**s3_data) as s3:
        try:
            file_key = f"docsgenerate/{filename}"
            if type_doc is TypeDoc.html:
                s3_ob = await s3.get_object(Bucket=bucket_name, Key=file_key)
                body = await s3_ob['Body'].read()
                return Response(content=body, media_type="text/html",
                                headers={'Response-content-disposition': 'attachment'})
            if type_doc is TypeDoc.pdf:
                return {
                    "data": {
                        "url": s3_data["endpoint_url"] + "/" + file_key
                    }
                }
        except Exception as err:
            return HTTPException(status_code=404, detail="Такого документа не существует")


@router.get('/docgenerated/', status_code=status.HTTP_200_OK)
async def get_doc_generate_list(cashbox: int, tags: str = None, limit: int = 100, offset: int = 0):
    """Получение списка генераций"""
    if tags:
        tags = list(map(lambda x: x.strip().lower(), tags.replace(' ', '').strip().split(',')))
        filter_tags = list(map(lambda x: doc_generated.c.tags.like(f'%{x}%'), tags))
        query = doc_generated.select().where(doc_generated.c.cashbox_id == cashbox,
                                             *filter_tags).order_by(desc(doc_generated.c.created_at)).limit(
            limit).offset(offset)
        result = await database.fetch_all(query)
        return {'results': result}
    else:
        query = doc_generated.select().where(doc_generated.c.cashbox_id == cashbox).limit(limit).offset(offset)
        result = await database.fetch_all(query)
        return {'results': result}

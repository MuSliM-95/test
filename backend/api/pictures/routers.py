import aiofiles
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import Response
from uuid import uuid4
from os import environ

import aioboto3
import io

from database import db
from database.db import database, pictures

import api.pictures.schemas as schemas
from functions.filter_schemas import PicturesFiltersQuery

from functions.helpers import datetime_to_timestamp, get_entity_by_id
from functions.helpers import get_user_by_token

from ws_manager import manager
from sqlalchemy import select, func

router = APIRouter(tags=["pictures"])


s3_session = aioboto3.Session()


s3_data = {
    "service_name": "s3",
    "endpoint_url": environ.get("S3_URL"),
    "aws_access_key_id": environ.get("S3_ACCESS"),
    "aws_secret_access_key": environ.get("S3_SECRET"),
}

bucket_name = "5075293c-docs_generated"


@router.get("/pictures/{idx}/", response_model=schemas.Picture)
async def get_picture_by_id(token: str, idx: int):
    """Получение картинки по ID"""
    user = await get_user_by_token(token)
    picture_db = await get_entity_by_id(pictures, idx, user.cashbox_id)
    picture_db = datetime_to_timestamp(picture_db)
    return picture_db


@router.get("/photos/{filename}/")
async def get_picture_by_id(filename: str):
    """Получение картинки по ID"""
    async with s3_session.client(**s3_data) as s3:
        try:


            file_key = f"photos/{filename}"
            s3_ob = await s3.get_object(Bucket=bucket_name, Key=file_key)
            body = await s3_ob['Body'].read()

            return Response(content=body, media_type="image/jpg")

        except Exception as err:
            print(err)
            raise HTTPException(status_code=404, detail="Такой картинки не существует")


@router.get("/photos/link/{filename}/")
async def get_picture_link_by_id(filename: str):
    """Получение картинки по ID"""
    async with s3_session.client(**s3_data) as s3:
        try:
            file_key = f"photos/{filename}"
            url = await s3.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': file_key
                },
                ExpiresIn=None
            )
            return {
                "data": {
                    "url": url
                }
            }
        except Exception as err:
            print(err)
            raise HTTPException(status_code=404, detail="Такой картинки не существует")


@router.get("/pictures/", response_model=schemas.PictureListGet)
async def get_pictures(
    token: str,
    limit: int = 100,
    offset: int = 0,
    filters: PicturesFiltersQuery = Depends(),
):
    """Получение списка картинок"""
    user = await get_user_by_token(token)

    filters_list = []
    if filters.entity:
        filters_list.append(pictures.c.entity == filters.entity)
    if filters.entity_id:
        filters_list.append(pictures.c.entity_id == filters.entity_id)

    query = (
        pictures.select()
        .where(
            pictures.c.owner == user.id,
            pictures.c.is_deleted.is_not(True),
            *filters_list,
        )
        .limit(limit)
        .offset(offset)
    )

    pictures_db = await database.fetch_all(query)
    pictures_db = [*map(datetime_to_timestamp, pictures_db)]

    query = (
        select(func.count(pictures.c.id))
        .where(
            pictures.c.owner == user.id,
            pictures.c.is_deleted.is_not(True),
            *filters_list,
        )
    )

    pictures_db_c = await database.fetch_one(query)

    return {"result": pictures_db, "count": pictures_db_c.count_1}


@router.post("/pictures/", response_model=schemas.Picture)
async def new_picture(
    token: str,
    entity: str,
    entity_id: int,
    is_main: bool = False,
    file: UploadFile = File(None),
):
    """Создание картинки"""
    if not file:
        raise HTTPException(status_code=422, detail="Вы не загрузили картинку.")
    if "image" not in file.content_type:
        raise HTTPException(status_code=422, detail="Вы загрузили не картинку.")
    if entity not in dir(db):
        raise HTTPException(status_code=422, detail="Такой entity не существует")

    user = await get_user_by_token(token)

    file_link = (
        f"photos/{entity}_{entity_id}_{uuid4().hex[:8]}.{file.filename.split('.')[-1]}"
    )

    file_bytes = await file.read()
    file_size = len(file_bytes)

    async with s3_session.client(**s3_data) as s3:
        await s3.upload_fileobj(io.BytesIO(file_bytes), bucket_name, file_link)

    # async with aiofiles.open(file_link, "wb") as file_out:
    #     await file_out.write(file_bytes)
    # await file.close()

    picture_values = {
        "entity": entity,
        "entity_id": entity_id,
        "is_main": is_main,
        "owner": user.id,
        "url": file_link,
        "size": file_size,
        "cashbox": user.cashbox_id,
        "is_deleted": False
    }

    query = pictures.insert().values(picture_values)
    picture_id = await database.execute(query)
    
    query = pictures.select().where(
        pictures.c.id == picture_id,
        pictures.c.owner == user.id,
        pictures.c.is_deleted.is_not(True),
    )
    picture_db = await database.fetch_one(query)

    if not picture_db:
        raise HTTPException(
            status_code=404, detail=f"У вас нет {pictures.name.rstrip('s')} с таким id."
        )

    picture_db = datetime_to_timestamp(picture_db)

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "pictures",
            "result": picture_db,
        },
    )

    return picture_db


@router.patch("/pictures/{idx}/", response_model=schemas.Picture)
async def edit_picture(
    token: str,
    idx: int,
    picture: schemas.PictureEdit,
):
    """Редактирование картинки"""
    user = await get_user_by_token(token)
    picture_db = await get_entity_by_id(pictures, idx, user.cashbox_id)
    picture_values = picture.dict(exclude_unset=True)

    if picture_values:
        query = (
            pictures.update()
            .where(pictures.c.id == idx, pictures.c.owner == user.id)
            .values(picture_values)
        )
        await database.execute(query)
        picture_db = await get_entity_by_id(pictures, idx, user.cashbox_id)

    picture_db = datetime_to_timestamp(picture_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "pictures", "result": picture_db},
    )

    return picture_db


@router.delete("/pictures/{idx}/", response_model=schemas.Picture)
async def delete_picture(token: str, idx: int):
    """Удаление картинки"""
    user = await get_user_by_token(token)

    await get_entity_by_id(pictures, idx, user.cashbox_id)

    query = (
        pictures.update()
        .where(pictures.c.id == idx, pictures.c.owner == user.id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = pictures.select().where(pictures.c.id == idx, pictures.c.owner == user.id)
    picture_db = await database.fetch_one(query)
    picture_db = datetime_to_timestamp(picture_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "pictures",
            "result": picture_db,
        },
    )

    return picture_db

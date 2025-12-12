import io
import re
from datetime import datetime
from os import environ
from uuid import uuid4
from zoneinfo import ZoneInfo

import aioboto3
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import Response
from sqlalchemy import func, select

import api.pictures.schemas as schemas
from database import db
from database.db import database, pictures
from functions.filter_schemas import PicturesFiltersQuery
from functions.helpers import (
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
)
from ws_manager import manager


# Поддерживаемые MIME-типы и соответствующие расширения
ALLOWED_CONTENT_TYPES = {
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "application/pdf": "pdf",
}

router = APIRouter(tags=["pictures"])

s3_session = aioboto3.Session()

s3_data = {
    "service_name": "s3",
    "endpoint_url": environ.get("S3_URL"),
    "aws_access_key_id": environ.get("S3_ACCESS"),
    "aws_secret_access_key": environ.get("S3_SECRET"),
}

bucket_name = "5075293c-docs_generated"


async def get_picture_by_filename(filename: str, cashbox_id: int):
    """Найти картинку по имени файла и cashbox_id"""
    safe_filename = filename.replace("%", "\\%").replace("_", "\\_")
    query = pictures.select().where(
        pictures.c.url.like(f"%/{safe_filename}", escape="\\"),
        pictures.c.cashbox == cashbox_id,
        pictures.c.is_deleted.is_not(True)
    )
    return await database.fetch_one(query)


@router.get("/pictures/{idx}/", response_model=schemas.Picture)
async def get_picture_by_id(token: str, idx: int):
    """Получение картинки по ID"""
    user = await get_user_by_token(token)
    picture_db = await get_entity_by_id(pictures, idx, user.cashbox_id)
    picture_db = datetime_to_timestamp(picture_db)
    return picture_db


@router.get("/photos/{filename}/")
async def get_picture_link_by_id(filename: str, token: str):
    """Получение файла по имени (безопасно, с проверкой прав)"""
    if not re.match(r"^[a-f0-9]{32}\.(jpg|jpeg|png|gif|pdf)$", filename, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Недопустимое имя файла")

    user = await get_user_by_token(token)
    picture = await get_picture_by_filename(filename, user.cashbox_id)

    if not picture:
        raise HTTPException(status_code=404, detail="Файл не найден")

    async with s3_session.client(**s3_data) as s3:
        try:
            s3_obj = await s3.get_object(Bucket=bucket_name, Key=picture.url)
            body = await s3_obj['Body'].read()
        except Exception as e:
            print(f"S3 error: {e}")
            raise HTTPException(status_code=500, detail="Ошибка при загрузке файла")

    # Определяем media_type
    if filename.lower().endswith('.png'):
        media_type = "image/png"
    elif filename.lower().endswith('.gif'):
        media_type = "image/gif"
    elif filename.lower().endswith('.pdf'):
        media_type = "application/pdf"
    else:
        media_type = "image/jpeg"

    return Response(content=body, media_type=media_type)


@router.get("/photos/link/{filename}/")
async def get_picture_link_by_filename_endpoint(filename: str, token: str):
    """Получение пресайгнутой ссылки по имени файла"""
    if not re.match(r"^[a-f0-9]{32}\.(jpg|jpeg|png|gif|pdf)$", filename, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Недопустимое имя файла")

    user = await get_user_by_token(token)
    picture = await get_picture_by_filename(filename, user.cashbox_id)

    if not picture:
        raise HTTPException(status_code=404, detail="Файл не найден")

    async with s3_session.client(**s3_data) as s3:
        try:
            url = await s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': picture.url},
                ExpiresIn=3600  # 1 час
            )
        except Exception as e:
            print(f"S3 presign error: {e}")
            raise HTTPException(status_code=500, detail="Не удалось сгенерировать ссылку")

    return {"data": {"url": url}}


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
    """Создание картинки с организацией по дате и cashbox_id"""
    if not file:
        raise HTTPException(status_code=422, detail="Вы не загрузили картинку.")
    if file.content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=422,
            detail="Неподдерживаемый тип файла. Разрешены: JPEG, PNG, GIF, PDF."
        )

    if entity not in dir(db):
        raise HTTPException(status_code=422, detail="Такой entity не существует")

    user = await get_user_by_token(token)

    # Получаем текущую дату
    moscow_tz = ZoneInfo("Europe/Moscow")
    now = datetime.now(moscow_tz)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    cashbox_id = str(user.cashbox_id)

    ext = ALLOWED_CONTENT_TYPES[file.content_type]

    filename = f"{uuid4().hex}.{ext}"
    file_key = f"photos/{year}/{month}/{day}/{cashbox_id}/{filename}"

    file_bytes = await file.read()
    file_size = len(file_bytes)

    # Загружаем в S3
    try:
        async with s3_session.client(**s3_data) as s3:
            await s3.upload_fileobj(io.BytesIO(file_bytes), bucket_name, file_key)
    except Exception as e:
        # Если S3 недоступен или вернул ошибку — НЕ сохраняем в БД
        print(f"Ошибка загрузки в S3: {e}")
        raise HTTPException(
            status_code=502,
            detail="Не удалось сохранить изображение на сервере. Повторите попытку позже."
        )

    # === Только после успешной загрузки — записываем в БД ===
    picture_values = {
        "entity": entity,
        "entity_id": entity_id,
        "is_main": is_main,
        "owner": user.id,
        "url": file_key,  # полный путь в S3
        "size": file_size,
        "cashbox": user.cashbox_id,
        "is_deleted": False
    }

    try:
        query = pictures.insert().values(picture_values)
        picture_id = await database.execute(query)

        query = pictures.select().where(
            pictures.c.id == picture_id,
            pictures.c.owner == user.id,
            pictures.c.is_deleted.is_not(True),
        )
        picture_db = await database.fetch_one(query)

        if not picture_db:
            # Теоретически не должно происходить, но на всякий случай
            raise HTTPException(
                status_code=500,
                detail="Ошибка при сохранении метаданных изображения."
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

    except Exception as db_err:
        # попытка удалить файл из S3 при ошибке БД (желательно)
        try:
            async with s3_session.client(**s3_data) as s3:
                await s3.delete_object(Bucket=bucket_name, Key=file_key)
        except Exception as cleanup_err:
            print(f"Не удалось удалить файл после ошибки БД: {cleanup_err}")
        raise HTTPException(
            status_code=500,
            detail="Ошибка при сохранении данных изображения. Файл отменён."
        )


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

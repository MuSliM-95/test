import json
import uuid

from fastapi import APIRouter, HTTPException, UploadFile, File, Response
from fastapi.responses import JSONResponse
from sqlalchemy import select

from api.apple_wallet_card_settings.schemas import WalletCardSettings, WalletCardSettingsCreate, \
    WalletCardSettingsUpdate
from database.db import users_cboxes_relation, database, apple_wallet_card_settings

router = APIRouter(prefix='/apple_wallet_card_settings', tags=['apple_wallet_card_settings'])

UPLOAD_DIR = '/backend/photos'

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    # Простая валидация типа (при необходимости расширите)
    if file.content_type not in ("image/png", "image/jpeg", "image/svg+xml", "image/webp", "application/octet-stream"):
        # допускаем generic binary если нужно
        # при желании вернуть 415 Unsupported Media Type
        return Response(status_code=415)

    # Генерируем уникальное имя файла с сохранением расширения
    unique_name = f"{uuid.uuid4().hex}-{file.filename.split('.')[0]}.{file.filename.split('.')[-1]}"
    dest_path = f'{UPLOAD_DIR}/{unique_name}'

    try:
        with open(dest_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)  # читать по 1 MiB
                if not chunk:
                    break
                f.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to save file")

    return JSONResponse(content={"path": dest_path})

@router.get('', response_model=WalletCardSettings)
async def get_apple_wallet_card_settings(token: str):
    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    settings_query = select(
        apple_wallet_card_settings.c.data
    ).where(apple_wallet_card_settings.c.cashbox_id == user.cashbox_id)
    settings = await database.fetch_one(settings_query)

    if not settings:
        setting_stmt = apple_wallet_card_settings.insert().values(
            WalletCardSettingsCreate(
                cashbox_id=user.cashbox_id,
                data=WalletCardSettings()
            ).dict()
        ).returning(apple_wallet_card_settings.c.data)
        settings = await database.execute(setting_stmt)
        return WalletCardSettings(**json.loads(settings))

    return WalletCardSettings(**json.loads(settings.data))

@router.post('', response_model=WalletCardSettings)
async def create_apple_wallet_card_settings(token: str, settings: WalletCardSettings):
    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    setting_stmt = apple_wallet_card_settings.insert().values(
        WalletCardSettingsCreate(
            cashbox_id=user.cashbox_id,
            data=settings
        ).dict()
    ).returning(apple_wallet_card_settings.c.data)
    settings = await database.execute(setting_stmt)

    return WalletCardSettings(**json.loads(settings))

@router.patch('', response_model=WalletCardSettings)
async def update_apple_wallet_card_settings(token: str, settings: WalletCardSettings):
    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    settings_stmt = apple_wallet_card_settings.update().values(
        WalletCardSettingsUpdate(
            cashbox_id=user.cashbox_id,
            data=settings.dict(exclude_unset=True)
        ).dict()
    ).returning(apple_wallet_card_settings.c.data)
    settings = await database.execute(settings_stmt)

    return WalletCardSettings(**json.loads(settings))

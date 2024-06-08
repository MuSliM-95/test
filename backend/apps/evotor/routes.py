from fastapi import APIRouter, HTTPException, Depends, Request

from api.loyality_cards.routers import get_cards
from .schemas import EvotorInstallEvent, EvotorUserToken, ListEvotorNomenclature
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from database.db import database, integrations, integrations_to_cashbox, evotor_credentials, users_cboxes_relation
from functions.helpers import get_user_by_token
from ws_manager import manager
from sqlalchemy import or_, and_, select
from api.loyality_cards.schemas import LoyalityCardFilters


security = HTTPBearer()


async def has_access(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        token_db = await database.fetch_one(integrations.select().where(integrations.c.id == 2))
        if token_db:
            assert token == token_db.get("client_secret")
        else:
            raise AssertionError("not found integration")
    except AssertionError as e:
        raise HTTPException(
            status_code=401, detail=str(e))


async def has_user(req: Request):
    try:
        evotor_user_id = req.headers.get("x-evotor-user-id")

        user_cashbox = await database.fetch_one(
            select(evotor_credentials.c.userId,integrations_to_cashbox.c.token).
            select_from(evotor_credentials).
            join(integrations_to_cashbox, evotor_credentials.c.integration_cashboxes == integrations_to_cashbox.c.id).
            where(evotor_credentials.c.userId == evotor_user_id)
        )
        print(user_cashbox)
        if user_cashbox:
            return user_cashbox.get("token")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=432, detail="ошибка аутентификации пользователя Эвотор (неверный userId)")



router_auth = APIRouter(tags=["Evotor hook"], dependencies = [Depends(has_access)])
router = APIRouter(tags=["Evotor hook"])


@router_auth.post("/evotor/nomenclature")
async def events(data: ListEvotorNomenclature, req: Request):
    print(data, req.headers)


@router_auth.post("/evotor/events")
async def events(data: EvotorInstallEvent, req: Request):
    print(data, req.headers)


@router_auth.post("/evotor/user/token")
async def user_token(data: EvotorUserToken, req: Request):
    credential_check = await database.fetch_one(evotor_credentials.
                                                select().
                                                where(evotor_credentials.c.evotor_token == data.evotor_token))
    if not credential_check:
        await database.execute(evotor_credentials.
                               insert().
                               values({"evotor_token": data.evotor_token, "userId": data.userId, "status": False}))
    else:
        await database.execute(evotor_credentials.
                               update().
                               where(evotor_credentials.c.id == credential_check.get("id")).
                               values({"evotor_token": data.evotor_token}))


@router_auth.post("/evotor/loyality_cards/")
async def loyality_cards(
        limit: int = 100,
        offset: int = 0,
        filters_q: LoyalityCardFilters = Depends(),
        token: str = Depends(has_user)):
    return await get_cards(token=token, limit=limit, offset=offset, filters_q=filters_q)


@router.get("/evotor/integration/on")
async def integration_on(token: str, id_integration: int):

    """Установка связи аккаунта пользователя и интеграции"""

    user = await get_user_by_token(token)

    try:
        check = await database.fetch_one(integrations_to_cashbox.select().where(and_(
            integrations_to_cashbox.c.integration_id == id_integration,
            integrations_to_cashbox.c.installed_by == user.id
        )))
        if check:
            await database.execute(integrations_to_cashbox.update().where(and_(
                integrations_to_cashbox.c.integration_id == id_integration,
                integrations_to_cashbox.c.installed_by == user.id
            )).values({'status': True}))
        else:
            await database.execute(integrations_to_cashbox.insert().values({
                'integration_id': id_integration,
                'installed_by': user.get('id'),
                'deactivated_by': user.get('id'),
                'status': True,
            }))

        await manager.send_message(user.token,
                                    {"action": "on",
                                     "target": "IntegrationEvotor",
                                     "integration_status": True,
                                     "integration_isAuth": True
                                     })
        return {'result': 'ok'}
    except:
        raise HTTPException(status_code = 422, detail = "ошибка установки связи аккаунта пользователя и интеграции")

@database.transaction()
@router.get("/evotor/integration/off")
async def integration_off(token: str, id_integration: int):

    """Удаление связи аккаунта пользователя и интеграции"""

    user = await get_user_by_token(token)

    try:
        integration_cashbox = await database.fetch_one(integrations_to_cashbox.select().where(and_(
            integrations_to_cashbox.c.integration_id == id_integration,
            integrations_to_cashbox.c.installed_by == user.id
        )))

        await database.execute(integrations_to_cashbox.update().where(and_(
            integrations_to_cashbox.c.integration_id == id_integration,
            integrations_to_cashbox.c.installed_by == user.id
        )).values({
            'status': False
        }))

        await database.execute(evotor_credentials.
                               update().
                               where(evotor_credentials.c.integration_cashboxes == integration_cashbox.get("id")).
                               values({"status": False}))

        await manager.send_message(user.token,
                                    {"action": "off", "target": "IntegrationEvotor", "integration_status": False})

        return {'isAuth': False}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=422, detail="ошибка удаления связи аккаунта пользователя и интеграции")

@database.transaction()
@router.get("/evotor/integration/install")
async def install(token: str, evotor_token: str, id_integration: int):
    user = await get_user_by_token(token)
    try:
        integration_cashbox = await database.fetch_one(integrations_to_cashbox.select().where(and_(
            integrations_to_cashbox.c.integration_id == id_integration,
            integrations_to_cashbox.c.installed_by == user.id)))

        await database.execute(evotor_credentials.
                               update().
                               where(evotor_credentials.c.evotor_token == evotor_token).
                               values(
            {
                "integration_cashboxes": integration_cashbox.get("id"),
                "status": True
            }
        ))
    except:
        raise HTTPException(status_code=422, detail = "приложение не установлено в Эвотор")


@router.get("/evotor/integration/check")
async def check(token: str, id_integration: int):

    """Проверка установлена или нет интеграция у клиента"""

    user = await get_user_by_token(token)

    check = await database.fetch_one(integrations_to_cashbox.select().where(and_(
        integrations_to_cashbox.c.integration_id == id_integration,
        integrations_to_cashbox.c.installed_by == user.id
    )))
    if check is None:
        raise HTTPException(status_code = 204, detail = "integration not installed by chashbox")

    message = {
        "action": "check",
        "target": "IntegrationEvotor",
        "integration_status": check.get('status'),
    }

    isAuth = await database.fetch_one(
            evotor_credentials.
            select().
            where(
                and_(
                    evotor_credentials.c.integration_cashboxes == check.get("id"),
                    evotor_credentials.c.status.is_not(False)
                ))
        )

    if isAuth:
        message.update({'integration_isAuth': True})
    else:
        message.update({'integration_isAuth': False})
    await manager.send_message(user.token, message)
    return {"isAuth": message.get('integration_isAuth')}



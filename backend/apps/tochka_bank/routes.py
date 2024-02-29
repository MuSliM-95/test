import aiohttp
from jobs import scheduler, jobstore
from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse
from database.db import integrations, integrations_to_cashbox, users_cboxes_relation, database, tochka_bank_credentials
from datetime import datetime
from sqlalchemy import or_, and_, select

from functions.helpers import get_user_by_token
from ws_manager import manager

router = APIRouter(tags=["Tochka bank"])


async def integration_info(cashbox_id, id_integration):
    query = select(integrations_to_cashbox.c.installed_by,
                   users_cboxes_relation.c.token,
                   integrations_to_cashbox.c.id,
                   *integrations.columns) \
        .where(users_cboxes_relation.c.cashbox_id == cashbox_id) \
        .select_from(users_cboxes_relation) \
        .join(integrations_to_cashbox, users_cboxes_relation.c.id == integrations_to_cashbox.c.installed_by) \
        .select_from(integrations_to_cashbox) \
        .join(integrations, integrations.c.id == integrations_to_cashbox.c.integration_id).where(
        integrations.c.id == id_integration)
    return await database.fetch_one(query)


def refresh_token(integration_cashboxes):
    print(f'refresh token: {integration_cashboxes}')


@router.get("/bank/tochkaoauth")
async def tochkaoauth(code: str, state: int):

    """Hook для oauth банка"""

    user_integration = await integration_info(state, 1)

    async with aiohttp.ClientSession() as session:
        async with session.post(f'https://enter.tochka.com/connect/token', data = {
            'client_id': user_integration.get('client_app_id'),
            'client_secret': user_integration.get('client_secret'),
            'grant_type': 'authorization_code',
            'code': code,
            'scope': user_integration.get('scopes'),
        }, headers = {'Content-Type': 'application/x-www-form-urlencoded'}) as resp:
            token_json = await resp.json()
        await session.close()
    try:
        await database.execute(tochka_bank_credentials.insert().values({
            'access_token': token_json.get('access_token'),
            'refresh_token': token_json.get('refresh_token'),
            'integration_cashboxes': user_integration.get('id')}))
    except Exception as error:
        raise HTTPException(status_code=433, detail=str(error))
    print(token_json)
    if not scheduler.get_job(job_id = str(user_integration.get('installed_by'))):
        scheduler.add_job(refresh_token, 'interval', seconds = int(token_json.get('expires_in')), kwargs = {'integration_cashboxes': user_integration.get('id')}, name = 'refresh token', id = str(user_integration.get('installed_by')))
    else:
        scheduler.get_job(job_id = user_integration.get('installed_by')).reschedule('interval', seconds = int(token_json.get('expires_in')))
    return RedirectResponse(f'https://app.tablecrm.com/integrations?token={user_integration.get("token")}', status_code=302)


@router.get("/bank/get_oauth_link/")
async def get_token_for_scope(token: str, id_integration: int):

    """Получение токена для работы с разрешениями"""

    user = await get_user_by_token(token)
    user_integration = await integration_info(user.get('cashbox_id'), id_integration)

    async with aiohttp.ClientSession() as session:
        async with session.post(f'https://enter.tochka.com/connect/token', data = {
            'client_id': user_integration.get('client_app_id'),
            'client_secret': user_integration.get('client_secret'),
            'grant_type': 'client_credentials',
            'scope': user_integration.get('scopes'),
        }, headers = {'Content-Type': 'application/x-www-form-urlencoded'}) as resp:
            token_scope_json = await resp.json()
        await session.close()

    async with aiohttp.ClientSession() as session:
        async with session.post(f'https://enter.tochka.com/uapi/v1.0/consents', json = {
            "Data": {
                "permissions": [
                    "ReadAccountsBasic",
                    "ReadAccountsDetail",
                    "MakeAcquiringOperation",
                    "ReadAcquiringData",
                    "ReadBalances",
                    "ReadStatements",
                    "ReadCustomerData",
                    "ReadSBPData",
                    "EditSBPData",
                    "CreatePaymentForSign",
                    "CreatePaymentOrder",
                    "ManageWebhookData",
                    "ManageInvoiceData"
                ]
            }
        }, headers = {'Authorization': f'Bearer {token_scope_json.get("access_token")}', 'Content-Type': 'application/json'}) as resp:
            api_resp_json = await resp.json()
        await session.close()

        link = f'{user_integration.get( "url" )}authorize?' \
               f'client_id={api_resp_json.get("Data").get("clientId")}&' \
               f'response_type=code&' \
               f'redirect_uri={user_integration.get("redirect_uri")}&' \
               f'consent_id={api_resp_json.get("Data").get("consentId")}&' \
               f'scope={user_integration.get("scopes")}&' \
               f'state={user.get("cashbox_id")}'

    return {'link': link}


@router.get("/bank/check")
async def check(token: str, id_integration: int):

    """Проверка установлена или нет интеграция у клиента"""

    user = await get_user_by_token(token)

    check = await database.fetch_one(integrations_to_cashbox.select().where(and_(
        integrations_to_cashbox.c.integration_id == id_integration,
        integrations_to_cashbox.c.installed_by == user.id
    )))
    message = {
        "action": "check",
        "target": "IntegrationTochkaBank",
        "integration_status": check.get('status'),
    }
    if check.get('status'):
        isAuth = await database.fetch_one(
            tochka_bank_credentials.select().where(tochka_bank_credentials.c.integration_cashboxes == check.get("id"))
        )

        if isAuth:
            message.update({'integration_isAuth': True})
        else:
            message.update({'integration_isAuth': False})
    await manager.send_message(user.token, message)
    return {"isAuth": message.get('integration_isAuth')}


@router.get("/bank/integration_on")
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
                                    {"action": "on", "target": "IntegrationTochkaBank", "integration_status": True})
        return {'result': 'ok'}
    except:
        raise HTTPException(status_code = 422, detail = "ошибка установки связи аккаунта пользователя и интеграции")


@router.get("/bank/integration_off")
async def integration_off(token: str, id_integration: int):

    """Удаление связи аккаунта пользователя и интеграции"""

    user = await get_user_by_token(token)
    try:
        await database.execute(integrations_to_cashbox.update().where(and_(
            integrations_to_cashbox.c.integration_id == id_integration,
            integrations_to_cashbox.c.installed_by == user.id
        )).values({
            'status': False
        }))
        integration_cashbox = await database.fetch_one(integrations_to_cashbox.
                                                       select().
                                                       where(integrations_to_cashbox.c.installed_by == user.get("id")))
        await database.execute(tochka_bank_credentials.
                               delete().
                               where(tochka_bank_credentials.c.integration_cashboxes == integration_cashbox.get("id")))
        await manager.send_message(user.token,
                                    {"action": "off", "target": "IntegrationTochkaBank", "integration_status": False})
        return {'isAuth': False}
    except:
        raise HTTPException(status_code=422, detail="ошибка удаления связи аккаунта пользователя и интеграции")


@router.get("/bank/accounts/")
async def accounts(token: str, id_integration: int):

    """Получение списка счетов аккаунта банка"""

    user = await get_user_by_token(token)

    query = ((select(integrations_to_cashbox.c.id, integrations_to_cashbox.c.status, tochka_bank_credentials.c.access_token).
             where(and_(
                integrations_to_cashbox.c.installed_by == user.id,
                integrations_to_cashbox.c.integration_id == id_integration
                ))
             ).select_from(integrations_to_cashbox).
             join(tochka_bank_credentials, integrations_to_cashbox.c.id == tochka_bank_credentials.c.integration_cashboxes)
             )
    credential = await database.fetch_one(query)

    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://enter.tochka.com/uapi/open-banking/v1.0/accounts',
                               headers={
                                   'Authorization': f'Bearer {credential.get("access_token")}',
                                   'Content-type': 'application/json'
                               }) as resp:
            accounts_json = await resp.json()
        await session.close()

    return {'result': accounts_json}


import aiohttp
from jobs import scheduler, jobstore
from fastapi import APIRouter, HTTPException, UploadFile, File
from database.db import integrations, integrations_to_cashbox, users_cboxes_relation, database
from datetime import datetime
from sqlalchemy import or_, and_, select

from functions.helpers import get_user_by_token


router = APIRouter(tags=["Tochka bank"])


def create_job(link):
    print(f'finish link: {link}')


@router.post("/get_oauth_link/")
async def get_token_for_scope(token: str, id_integration: int):

    """Получение токена для работы с разрешениями"""

    user = await get_user_by_token(token)
    query = select(integrations_to_cashbox.c.installed_by, *integrations.columns) \
        .where(users_cboxes_relation.c.cashbox_id == user.cashbox_id)\
        .select_from(users_cboxes_relation)\
        .join(integrations_to_cashbox, users_cboxes_relation.c.id == integrations_to_cashbox.c.installed_by)\
        .select_from(integrations_to_cashbox)\
        .join(integrations, integrations.c.id == integrations_to_cashbox.c.installed_by).where(integrations.c.id == id_integration)
    user_integration = await database.fetch_one(query)

    async with aiohttp.ClientSession() as session:
        async with session.post(f'https://enter.tochka.com/connect/token', data = {
            'client_id': user_integration.get('client_app_id'),
            'client_secret': user_integration.get('client_secret'),
            'grant_type': 'client_credentials',
            'scope': user_integration.get('scopes'),
        }, headers = {'Content-Type': 'application/x-www-form-urlencoded'}) as resp:
            token_scope_json = await resp.json()
        await session.close()
    print(token_scope_json.get("access_token"))
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
            print(resp.request_info)
            api_resp_json = await resp.json()
        await session.close()
        link = f'{user_integration.get( "url" )}authorize?' \
               f'client_id={api_resp_json.get("Data").get("clientId")}&' \
               f'response_type=code&' \
               f'redirect_uri={user_integration.get("redirect_uri")}&' \
               f'consent_id={api_resp_json.get("Data").get("consentId")}&' \
               f'scope={user_integration.get("scopes")}'
        # scheduler.add_job(create_job, 'interval', seconds = 20, kwargs = {'link': link}, name = 'update token', id = api_resp_json.get("Data").get("clientId"))
    return {'result': link}




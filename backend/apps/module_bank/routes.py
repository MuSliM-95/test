from datetime import datetime

import aiohttp
from fastapi import APIRouter, HTTPException
from starlette.responses import RedirectResponse

from apps.tochka_bank.routes import integration_info
from database.db import database, pboxes, module_bank_credentials, module_bank_accounts
from functions.helpers import get_user_by_token

router = APIRouter(tags=["Module bank"])


@router.get("/bank/moduloauth")
async def moduloauth(code: str, state: int):
    """Hook для oauth банка"""

    user_integration = await integration_info(state, 3)

    if not user_integration:
        raise HTTPException(status_code=432, detail=f"user not found with integration")

    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.post(
                f'https://api.modulbank.ru/v1/oauth/token',
                data={
                    'code': code,
                    'clientId': user_integration.get('client_app_id'),
                    'clientSecret': user_integration.get('client_secret'),
                },
                headers={'Content-type': 'application/json'}
        ) as resp:
            token_json = await resp.json()
    if token_json.get('error'):
        raise HTTPException(status_code=432, detail=f"error OAuth2 {token_json.get('error')}")
    try:
        credential = await database.fetch_one(module_bank_credentials.select(
            module_bank_credentials.c.integration_cashboxes == user_integration.get('id')))
        if not credential:
            credentials_id = await database.execute(module_bank_credentials.insert().values({
                'access_token': token_json.get('access_token'),
                'integration_cashboxes': user_integration.get('id')}))
        else:
            await database.execute(module_bank_credentials.update().where(
                module_bank_credentials.c.integration_cashboxes == user_integration.get('id')).values({
                'access_token': token_json.get('access_token')}))
            credentials_id = credential.get('id')
    except Exception as error:
        raise HTTPException(status_code=433, detail=str(error))

    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.get(f'https://api.modulbank.ru/v1/account-info',
                               headers={
                                   'Authorization': f'Bearer {token_json.get("access_token")}',
                                   'Content-type': 'application/json'
                               }) as resp:
            companies_json = await resp.json()
    for company in companies_json:
        for bank_account in company.get("bankAccounts"):
            data = {
                'name': f"Счет банк Модуль №{bank_account.get('id')}",
                'start_balance': 0,
                'cashbox': state,
                'balance': bank_account.get("balance"),
                'update_start_balance': int(datetime.utcnow().timestamp()),
                'update_start_balance_date': int(datetime.utcnow().timestamp()),
                'created_at': int(datetime.utcnow().timestamp()),
                'updated_at': int(datetime.utcnow().timestamp()),
                'balance_date': 0
            }
            account_db = await database.fetch_one(
                module_bank_accounts.select().where(module_bank_accounts.c.accountId == bank_account.get('id')))
            if not account_db:
                id_paybox = await database.execute(pboxes.insert().values(data))
                await database.execute(module_bank_accounts.insert().values(
                    {
                        'payboxes_id': id_paybox,
                        'module_bank_credential_id': credentials_id,
                        'accountName': bank_account.get('accountName'),
                        'bankBic': bank_account.get('bankBic'),
                        'bankInn': bank_account.get('bankInn'),
                        'bankKpp': bank_account.get('bankKpp'),
                        'bankCorrespondentAccount': bank_account.get('bankCorrespondentAccount'),
                        'bankName': bank_account.get('bankName'),
                        'beginDate': bank_account.get('beginDate'),
                        'category': bank_account.get('category'),
                        'currency': bank_account.get('currency'),
                        'accountId': bank_account.get('id'),
                        'number': bank_account.get('number'),
                        'status': bank_account.get('status'),
                        'is_deleted': False,
                        'is_active': False
                    }
                ))
            else:
                del data['created_at']
                id_paybox = await database.execute(
                    pboxes.update().where(pboxes.c.id == account_db.get('payboxes_id')).values(data))
                await database.execute(
                    module_bank_accounts.update().where(module_bank_accounts.c.id == account_db.get('id')).values(
                        {
                            'payboxes_id': id_paybox,
                            'module_bank_credential_id': credentials_id,
                            'accountName': bank_account.get('accountName'),
                            'bankBic': bank_account.get('bankBic'),
                            'bankInn': bank_account.get('bankInn'),
                            'bankKpp': bank_account.get('bankKpp'),
                            'bankCorrespondentAccount': bank_account.get('bankCorrespondentAccount'),
                            'bankName': bank_account.get('bankName'),
                            'beginDate': bank_account.get('beginDate'),
                            'category': bank_account.get('category'),
                            'currency': bank_account.get('currency'),
                            'accountId': bank_account.get('id'),
                            'number': bank_account.get('number'),
                            'status': bank_account.get('status'),
                            'is_deleted': False,
                            'is_active': False
                        }
                    ))

    return RedirectResponse(f'https://app.tablecrm.com/integrations?token={user_integration.get("token")}')


@router.get("/bank/get_oauth_link/")
async def get_token_for_scope(token: str, id_integration: int):

    """Получение токена для работы с разрешениями"""

    user = await get_user_by_token(token)
    user_integration = await integration_info(user.get('cashbox_id'), id_integration)

    link = f'{user_integration.get("url")}authorize?' \
           f'clientId={user_integration.get("client_app_id")}&' \
           f'redirectUri={user_integration.get("redirect_uri")}&' \
           f'scope={user_integration.get("scopes")}&' \
           f'state={user.get("cashbox_id")}'

    return {'link': link}

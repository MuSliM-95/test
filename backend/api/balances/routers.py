from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException
from sqlalchemy import func, select

from const import PAY_LINK
from database.db import users_cboxes_relation, database, accounts_balances, tariffs
from api.balances.schemas import AccountInfo

router = APIRouter(tags=["account"])


@router.get("/account/info", response_model=AccountInfo)
async def get_account_info(token: str):
    """Получение информации об аккаунте и оплате"""
    query = users_cboxes_relation.select(
        users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            balance = await database.fetch_one(
                accounts_balances.select(accounts_balances.c.cashbox==user.cashbox_id)
            )
            if balance:
                balance_tariff = await database.fetch_one(
                    tariffs.select().where(tariffs.c.id == balance.tariff)
                )
                count_query = (
                    select(func.count(users_cboxes_relation.c.id))
                    .where(users_cboxes_relation.c.cashbox_id == balance.cashbox)
                )
                users_quantity = await database.execute(count_query)
                demo_expiration = int(
                    (datetime.fromtimestamp(balance.created_at) + timedelta(days=balance_tariff.demo_days))
                    .timestamp()
                )
                demo_left = demo_expiration - datetime.utcnow().timestamp()
                info = AccountInfo(
                    type = balance.tariff_type,
                    demo_expiration = demo_expiration,
                    demo_left = demo_left if demo_left >= 0 else 0,
                    balance = balance.balance,
                    users = users_quantity,
                    price = balance_tariff.price,
                    is_per_user = balance_tariff.per_user,
                    tariff = balance_tariff.name,
                    link_for_pay = PAY_LINK,
                    demo_period = balance_tariff.demo_days,
                )
                return info
            raise HTTPException(status_code=404, detail="Нет доп. информации о вашем аккаунте")
    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

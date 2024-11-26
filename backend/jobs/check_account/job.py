from datetime import datetime, timedelta

from const import PAID, DEMO
from database.db import accounts_balances, database, tariffs
from functions.account import make_account


async def check_account():
    await database.connect()

    balances = await database.fetch_all(accounts_balances.select())
    for balance in balances:
        if balance.tariff_type == DEMO:
            tariff = await database.fetch_one(tariffs.select().where(tariffs.c.id == balance.tariff))
            if datetime.utcnow() >= datetime.fromtimestamp(balance.created_at) + timedelta(days=tariff.demo_days):
                await make_account(balance)
        elif balance.tariff_type == PAID:
            await make_account(balance)

import os
from datetime import datetime, timedelta

from sqlalchemy import desc
from sqlalchemy.exc import DatabaseError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from pytz import utc
from const import PAID, DEMO, RepeatPeriod
from database.db import engine, accounts_balances, database, tariffs, payments, loyality_transactions, loyality_cards, cboxes
from functions.account import make_account
from functions.goods_distribution import process_distribution
from functions.gross_profit import process_gross_profit_report
from functions.payments import clear_repeats, repeat_payment

scheduler = AsyncIOScheduler(
    {"apscheduler.job_defaults.max_instances": 25}, timezone=utc
)
jobstore = SQLAlchemyJobStore(engine=engine)
try:
    jobstore.remove_all_jobs()
except DatabaseError:
    pass
scheduler.add_jobstore(jobstore)


def add_job_to_sched(func, **kwargs):
    scheduler.add_job(func, **kwargs)


accountant_interval = int(os.getenv("ACCOUNT_INTERVAL", default=300))


# accountant_interval = int(os.getenv("ACCOUNT_INTERVAL", default=300))
# amo_interval = int(os.getenv("AMO_CONTACTS_IMPORT_FREQUENCY_SECONDS", default=120))


@scheduler.scheduled_job("interval", seconds=accountant_interval)
async def check_account():
    await database.connect()
    balances = await database.fetch_all(accounts_balances.select())
    tariff = await database.fetch_one(tariffs.select().where(tariffs.c.actual == True))
    for balance in balances:
        if balance.tariff_type == DEMO:
            now = datetime.utcnow()
            if now >= datetime.fromtimestamp(balance.created_at) + timedelta(
                    days=tariff.demo_days
            ):
                await make_account(balance)
        elif balance.tariff_type == PAID:
            await make_account(balance)


@scheduler.scheduled_job("interval", seconds=60)
async def autoburn():
    await database.connect()

    all_cards = await database.fetch_all(loyality_cards.select().where(loyality_cards.balance > 0))
    for card in all_cards:
        card_id = card.id
        balance = card.balance
        lifetime = card.lifetime
        if lifetime:
            q = loyality_transactions.select().where(loyality_transactions.loyality_card_id == card_id)
            all_transactions = await database.fetch_all(q)
            total_accrual = 0
            for transaction in all_transactions:
                if transaction.created_at.timestamp() + lifetime < datetime.now().timestamp():
                    if transaction.type == "accrual":
                        total_accrual += transaction.amount
            burn_amount = balance - total_accrual
            if burn_amount > 0:
                new_balance = balance - burn_amount
                query = loyality_cards.update().where(loyality_cards.id == card_id).values({"balance": new_balance})
                await database.execute(query)

            cashbox = await database.fetch_one(cboxes.select().where(cboxes.c.id == card.cashbox_id))
            admin = cashbox.admin

            rubles_body = {
                "loyality_card_id": card_id,
                "loyality_card_number": card.card_number,
                "type": "autoburned",
                "name": f"Автосписание",
                "amount": burn_amount,
                "created_by_id": admin,
                "tags": "",
                "dated": datetime.now(),
                "cashbox": cashbox.id,
                "is_deleted": False,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "status": True,
                "autoburned": True,
            }
            await database.execute(loyality_transactions.insert().values(rubles_body))
            


# @scheduler.scheduled_job("interval", seconds=amo_interval)
# async def amo_import():
#     await database.connect()
#     balances = await database.fetch_all(accounts_balances.select())
#     tariff = await database.fetch_one(tariffs.select().where(tariffs.c.actual == True))
#     for balance in balances:
#         if balance.tariff_type == DEMO:
#             now = datetime.utcnow()
#             if now >= datetime.fromtimestamp(balance.created_at) + timedelta(
#                 days=tariff.demo_days
#             ):
#                 await make_account(balance)
#         elif balance.tariff_type == PAID:
#             await make_account(balance)


@scheduler.scheduled_job("interval", seconds=5)
async def repeat_payments():
    await database.connect()
    query = payments.select().filter(payments.c.repeat_period != None)
    payments_db = await database.fetch_all(query)
    for payment in payments_db:
        child_payments_query = (
            payments.select()
            .filter(payments.c.repeat_parent_id == payment.id)
            .order_by(desc(payments.c.created_at))
        )
        child_payments = await database.fetch_all(child_payments_query)
        last_payment = payment if not child_payments else child_payments[0]
        last_time = datetime.fromtimestamp(last_payment.created_at)
        now = datetime.utcnow()
        if payment.repeat_number and len(child_payments) >= payment.repeat_number:
            await clear_repeats(payment.id)
            continue
        if payment.repeat_first:
            if payment.repeat_first > now.timestamp():
                continue
            elif last_time.timestamp() < payment.repeat_first <= now.timestamp():
                await repeat_payment(last_payment, payment.id)
                continue
        # If the first_repeat already done or no first_repeat:
        if payment.repeat_period == RepeatPeriod.YEARLY:
            if not payment.repeat_month or not payment.repeat_day:
                continue
            if now.day == payment.repeat_day and now.month == payment.repeat_month:
                if now - timedelta(days=1) >= last_time:
                    await repeat_payment(last_payment, payment.id)
        elif payment.repeat_period == RepeatPeriod.MONTHLY:
            if not payment.repeat_day:
                continue
            if now.day == payment.repeat_day:
                if now - timedelta(days=1) >= last_time:
                    await repeat_payment(last_payment, payment.id)
        elif payment.repeat_period == RepeatPeriod.WEEKLY:
            if not payment.repeat_weekday:
                continue
            try:
                payment_weekdays = [
                    *map(
                        lambda x: int(x) if x else None,
                        payment.repeat_weekday.split(","),
                    )
                ]
            except ValueError as e:
                print("Error in payment_weekdays:", e)
                continue
            if now.weekday in payment_weekdays:
                if now - timedelta(days=1) >= last_time:
                    await repeat_payment(last_payment, payment.id)
        elif payment.repeat_period == RepeatPeriod.DAILY:
            if now - timedelta(days=1) >= last_time:
                await repeat_payment(last_payment, payment.id)
        elif payment.repeat_period == RepeatPeriod.HOURLY:
            if now - timedelta(hours=1) >= last_time:
                await repeat_payment(last_payment, payment.id)
        elif payment.repeat_period == RepeatPeriod.SECONDS:
            if not payment.repeat_seconds:
                continue
            if now - timedelta(seconds=payment.repeat_seconds) >= last_time:
                await repeat_payment(last_payment, payment.id)

        if payment.repeat_last and payment.repeat_last < now.timestamp():
            await clear_repeats(payment.id)


@scheduler.scheduled_job("interval", seconds=5)
async def distribution():
    await process_distribution()
    await process_gross_profit_report()

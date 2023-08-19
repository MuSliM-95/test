import os
from datetime import datetime, timedelta

from sqlalchemy import desc
from sqlalchemy.exc import DatabaseError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from pytz import utc
from const import PAID, DEMO, RepeatPeriod
from database.db import engine, accounts_balances, database, tariffs, payments
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

import asyncio
import os
import re
from datetime import datetime, timedelta, timezone
from time import sleep
from typing import List, Union, Any, Dict
from itertools import zip_longest
from asyncpg import create_pool

import aiohttp
from apscheduler.jobstores.base import JobLookupError
from databases.backends.postgres import Record
from dateutil.relativedelta import relativedelta
from fastapi.exceptions import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.docs_sales.schemas import Item as goods_schema
from sqlalchemy import desc, select, or_, and_, alias, func, asc
from sqlalchemy.exc import DatabaseError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from pytz import utc

from api.contracts.schemas import PaymentType
from api.docs_warehouses.func_warehouse import call_type_movement
from api.payments.routers import read_payments_list, create_payment
from api.payments.schemas import PaymentCreate
from apps.tochka_bank.schemas import StatementData

from const import PAID, DEMO, RepeatPeriod
from database.db import engine, accounts_balances, database, tariffs, payments, loyality_transactions, loyality_cards, \
    cboxes, engine_job_store, tochka_bank_accounts, tochka_bank_credentials, pboxes, users_cboxes_relation, \
    entity_to_entity, tochka_bank_payments, contragents, docs_sales, docs_sales_settings, warehouse_balances, \
    docs_sales_goods, docs_sales_tags, docs_warehouse, nomenclature, SQLALCHEMY_DATABASE_URL, async_session_maker, \
    module_bank_operations, module_bank_accounts, module_bank_credentials, integrations_to_cashbox
from database.enums import Repeatability
from functions.account import make_account
from functions.filter_schemas import PaymentFiltersQuery
from functions.goods_distribution import process_distribution
from functions.gross_profit import process_gross_profit_report
from functions.helpers import init_statement, get_statement
from functions.payments import clear_repeats, repeat_payment
from functions.users import raschet
from jobs.autoburn_job.job import autoburn
from jobs.module_bank_job.job import module_bank_update_transaction
from jobs.tochka_bank_job.job import tochka_update_transaction
from jobs.check_account.job import check_account
from jobs.segment_jobs.job import segment_update
from jobs.avito_status_check_job.job import check_avito_accounts_status
from jobs.avito_auto_sync_chats_job.job import sync_avito_chats_and_messages

# Добавляем импорт для обновления времени смен
from api.employee_shifts.websocket_service import send_shift_time_updates

scheduler = AsyncIOScheduler(
    {"apscheduler.job_defaults.max_instances": 25}, timezone=utc
)
jobstore = SQLAlchemyJobStore(engine=engine_job_store)

scheduler.add_jobstore(jobstore)

try:
    try:
        jobstore.remove_job("check_account")
    except JobLookupError:
        pass
    try:
        jobstore.remove_job("autorepeat")
    except JobLookupError:
        pass
    try:
        jobstore.remove_job("repeat_payments")
    except JobLookupError:
        pass
    try:
        jobstore.remove_job("distribution")
    except JobLookupError:
        pass
except DatabaseError:
    pass


def add_job_to_sched(func, **kwargs):
    scheduler.add_job(func, **kwargs)


accountant_interval = int(os.getenv("ACCOUNT_INTERVAL", default=300))

scheduler.add_job(func=tochka_update_transaction, trigger='interval', minutes=5, id="tochka_update_transaction", max_instances=1, replace_existing=True)
scheduler.add_job(func=module_bank_update_transaction, trigger='interval', minutes=5, id="module_bank_update_transaction", max_instances=1, replace_existing=True)
scheduler.add_job(func=autoburn, trigger="interval", seconds=5, id="autoburn", max_instances=1, replace_existing=True)
scheduler.add_job(func=check_account, trigger="interval", seconds=accountant_interval, id="check_account", max_instances=1, replace_existing=True)
scheduler.add_job(func=segment_update, trigger="interval", seconds=60, id="segment_update", max_instances=1, replace_existing=True)

# Добавляем джоб для обновления времени смен каждую минуту
scheduler.add_job(func=send_shift_time_updates, trigger="interval", minutes=1, id="shift_time_updates", max_instances=1, replace_existing=True)

# Добавляем джоб для проверки статусов Avito аккаунтов каждые 5 минут
scheduler.add_job(func=check_avito_accounts_status, trigger="interval", minutes=5, id="avito_status_check", max_instances=1, replace_existing=True)

# Добавляем джоб для автоматической выгрузки чатов и сообщений из Avito каждые 5 минут
scheduler.add_job(func=sync_avito_chats_and_messages, trigger="interval", minutes=5, id="avito_auto_sync_chats", max_instances=1, replace_existing=True)

# accountant_interval = int(os.getenv("ACCOUNT_INTERVAL", default=300))
# amo_interval = int(os.getenv("AMO_CONTACTS_IMPORT_FREQUENCY_SECONDS", default=120))


# @scheduler.scheduled_job("interval", seconds=accountant_interval, id="check_account")
# async def check_account():
#     await database.connect()
#     balances = await database.fetch_all(accounts_balances.select())
#     tariff = await database.fetch_one(tariffs.select().where(tariffs.c.actual == True))
#     for balance in balances:
#         if balance.tariff_type == DEMO:
#             now = datetime.utcnow()
#             if now >= datetime.fromtimestamp(balance.created_at) + timedelta(
#                     days=tariff.demo_days
#             ):
#                 await make_account(balance)
#         elif balance.tariff_type == PAID:
#             await make_account(balance)
#
#
# class AutoRepeat:
#     def __init__(self, doc: Record, session: AsyncSession, date_now) -> None:
#         self.doc: Record = doc
#         self.last_created_at: datetime = doc.updated_at_1
#         self.session: AsyncSession = session
#         self.date_now = date_now
#
#     async def get_count_docs_sales(self, cashbox_id: int) -> int:
#         query = (
#             select(func.count(docs_sales.c.id))
#             .where(and_(
#                 docs_sales.c.cashbox == cashbox_id,
#                 docs_sales.c.is_deleted.is_not(True)
#             ))
#         )
#         result = await self.session.execute(query)
#         return result.scalar()
#
#     async def get_count_docs_warehouses(self, cashbox_id: int) -> int:
#         query = (
#             select(func.count(docs_warehouse.c.id))
#             .where(and_(
#                 docs_warehouse.c.cashbox == cashbox_id,
#                 docs_warehouse.c.is_deleted.is_not(True)
#             ))
#         )
#         result = await self.session.execute(query)
#         return result.scalar()
#
#     async def get_last_created_at(self) -> None:
#         query = (
#             select(docs_sales.c.created_at)
#             .where(docs_sales.c.parent_docs_sales == self.doc.id)
#             .order_by(asc(docs_sales.c.id))
#         )
#         result = await self.session.execute(query)
#         last_created_at = result.scalar()
#         if last_created_at:
#             self.last_created_at = last_created_at
#
#     def _check_start_date(self) -> bool:
#         if self.doc.repeatability_period is Repeatability.months:
#             if self.date_now.weekday() >= 5 and self.doc.transfer_from_weekends:
#                 return False
#         return self.last_created_at + relativedelta(
#             **{self.doc.repeatability_period: self.doc.repeatability_value}
#         ) <= self.date_now
#
#     async def _repeat(self):
#         user_query = (
#             users_cboxes_relation.select()
#             .where(users_cboxes_relation.c.id == self.doc.created_by)
#         )
#         result = await self.session.execute(user_query)
#         user = result.fetchone()
#
#         goods_query = (
#             docs_sales_goods.select()
#             .where(docs_sales_goods.c.docs_sales_id == self.doc.id)
#         )
#         result = await self.session.execute(goods_query)
#         docs_sales_goods_list = result.fetchall()
#
#         payment_query = (
#             payments.select()
#             .where(payments.c.docs_sales_id == self.doc.id)
#         )
#         result = await self.session.execute(payment_query)
#         payment_list = result.fetchall()
#
#         docs_warehouses_query = (
#             docs_warehouse.select()
#             .where(docs_warehouse.c.docs_sales_id == self.doc.id)
#         )
#         result = await self.session.execute(docs_warehouses_query)
#         docs_warehouse_list = result.fetchall()
#
#         count_docs_sales = await self.get_count_docs_sales(cashbox_id=self.doc.cashbox)
#         count_docs_warehouses = await self.get_count_docs_warehouses(cashbox_id=self.doc.cashbox)
#
#         docs_sales_body = {
#             "number": str(count_docs_sales + 1),
#             "dated": int(self.date_now.strftime("%s")),
#             "operation": self.doc.operation,
#             "tags": self.doc.tags if self.doc.repeatability_tags else "",
#             "comment": self.doc.comment,
#             "cashbox": self.doc.cashbox,
#             "contragent": self.doc.contragent,
#             "contract": self.doc.contract,
#             "organization": self.doc.organization,
#             "warehouse": self.doc.warehouse,
#             "parent_docs_sales": self.doc.id,
#             "autorepeat": True,
#             "status": self.doc.status,
#             "tax_included": self.doc.tax_included,
#             "tax_active": self.doc.tax_active,
#             "sales_manager": self.doc.sales_manager,
#             "sum": self.doc.sum,
#             "created_by": self.doc.created_by,
#         }
#         query = (
#             docs_sales
#             .insert()
#             .values(docs_sales_body)
#             .returning(docs_sales.c.id)
#         )
#         result = await self.session.execute(query)
#         await self.session.commit()
#         created_doc_id = result.scalar()
#
#         if self.doc.repeatability_tags and self.doc.tags:
#             tags_insert_list = [
#                 {"docs_sales_id": created_doc_id, "name": tag_name}
#                 for tag_name in self.doc.tags.split(",")
#             ]
#             if tags_insert_list:
#                 query = docs_sales_tags.insert(tags_insert_list)
#                 await self.session.execute(query)
#                 await self.session.commit()
#
#         items_sum = 0
#         goods_res = []
#         for item in docs_sales_goods_list:
#             item = goods_schema.parse_obj(item).dict()
#             item["docs_sales_id"] = created_doc_id
#             item["nomenclature"] = int(item["nomenclature"])
#             item.pop("id", None)
#             item.pop("nomenclature_name", None)
#             item.pop("unit_name", None)
#
#             query = docs_sales_goods.insert().values(item)
#             await self.session.execute(query)
#             await self.session.commit()
#             items_sum += item["price"] * item["quantity"]
#             if self.doc.warehouse is not None:
#                 query = (
#                     warehouse_balances.select()
#                     .where(
#                         warehouse_balances.c.warehouse_id == self.doc.warehouse,
#                         warehouse_balances.c.nomenclature_id == item["nomenclature"]
#                     )
#                     .order_by(desc(warehouse_balances.c.created_at))
#                 )
#                 result = await self.session.execute(query)
#                 last_warehouse_balance = result.fetchone()
#                 warehouse_amount = (
#                     last_warehouse_balance.current_amount
#                     if last_warehouse_balance
#                     else 0
#                 )
#
#                 query = warehouse_balances.insert().values(
#                     {
#                         "organization_id": self.doc.organization,
#                         "warehouse_id": self.doc.warehouse,
#                         "nomenclature_id": item["nomenclature"],
#                         "document_sale_id": created_doc_id,
#                         "outgoing_amount": item["quantity"],
#                         "current_amount": warehouse_amount - item["quantity"],
#                         "cashbox_id": self.doc.cashbox,
#                     }
#                 )
#                 await self.session.execute(query)
#                 await self.session.commit()
#
#                 query = (
#                     nomenclature.select()
#                     .where(nomenclature.c.id == item["nomenclature"])
#                 )
#                 result = await self.session.execute(query)
#                 nomenclature_db = result.fetchone()
#
#                 if nomenclature_db.type == "product":
#                     goods_res.append(
#                         {
#                             "price_type": 1,
#                             "price": 0,
#                             "quantity": item["quantity"],
#                             "unit": item["unit"],
#                             "nomenclature": item["nomenclature"]
#                         }
#                     )
#
#         for item in payment_list:
#             query = (
#                 payments.insert()
#                 .values({
#                     "contragent": item.contragent,
#                     "type": item.type,
#                     "name": f"Оплата по документу {docs_sales_body['number']}",
#                     "amount_without_tax": item.amount_without_tax,
#                     "tags": item.tags,
#                     "amount": item.amount,
#                     "tax": item.tax,
#                     "tax_type": item.tax_type,
#                     "article_id": item.article_id,
#                     "article": item.article,
#                     "paybox": item.paybox,
#                     "date": int(self.date_now.strftime("%s")),
#                     "account": item.account,
#                     "cashbox": item.cashbox,
#                     "is_deleted": False,
#                     "created_at": int(self.date_now.strftime("%s")),
#                     "updated_at": int(self.date_now.strftime("%s")),
#                     "status": self.doc.default_payment_status,
#                     "stopped": True,
#                     "docs_sales_id": created_doc_id,
#                 })
#                 .returning(payments.c.id)
#             )
#             result = await self.session.execute(query)
#             payment_id = result.scalar()
#
#             if self.doc.default_payment_status:
#                 query = (
#                     pboxes.update()
#                     .where(pboxes.c.id == item.paybox)
#                     .values(
#                         {"balance": pboxes.c.balance - item.amount}
#                     )
#                 )
#                 await self.session.execute(query)
#                 await self.session.commit()
#
#             query = (
#                 entity_to_entity.insert()
#                 .values({
#                     "from_entity": 7,
#                     "to_entity": 5,
#                     "cashbox_id": self.doc.cashbox,
#                     "type": "docs_sales_payments",
#                     "from_id": created_doc_id,
#                     "to_id": payment_id,
#                     "status": True,
#                     "delinked": False,
#                 })
#             )
#             await self.session.execute(query)
#             await self.session.commit()
#         await asyncio.gather(asyncio.create_task(raschet(user, user.token)))
#
#         query = (
#             docs_sales.update()
#             .where(docs_sales.c.id == created_doc_id)
#             .values({"sum": items_sum})
#         )
#         await self.session.execute(query)
#         await self.session.commit()
#
#         for item in docs_warehouse_list:
#             body = {
#                 "number": str(count_docs_warehouses + 1),
#                 "dated": int(self.date_now.strftime("%s")),
#                 "docs_purchases": item.docs_purchases,
#                 "to_warehouse": item.to_warehouse,
#                 "status": item.status,
#                 "contragent": item.contragent,
#                 "organization": item.organization,
#                 "operation": item.operation,
#                 "comment": item.comment,
#                 "warehouse": item.warehouse,
#                 "docs_sales_id": created_doc_id,
#                 "goods": goods_res,
#             }
#             await call_type_movement(item.operation, entity_values=body, token=user.token)
#
#         update_settings_query = (
#             docs_sales_settings
#             .update()
#             .where(docs_sales_settings.c.id == self.doc.id_1)
#             .values({
#                 "date_next_created": 0,
#                 "repeatability_count": self.doc.repeatability_count - 1
#             })
#         )
#         await self.session.execute(update_settings_query)
#         await self.session.commit()
#
#     async def start(self):
#         if (self.doc.date_next_created not in [None, 0] and datetime.fromtimestamp(
#                 self.doc.date_next_created).timestamp() <= self.date_now.timestamp()) \
#                 or self._check_start_date():
#             return await self._repeat()
#
#
# @scheduler.scheduled_job("interval", minutes=1, id="autorepeat", max_instances=1)
# async def autorepeat():
#     date_now = datetime.now(timezone(timedelta(hours=0)))
#
#     async with async_session_maker() as session:
#         query = (
#             select(docs_sales, docs_sales_settings)
#             .where(
#                 docs_sales_settings.c.repeatability_status.is_(True),
#                 docs_sales_settings.c.repeatability_count > 0
#             )
#             .join(docs_sales_settings, docs_sales.c.settings == docs_sales_settings.c.id)
#         )
#         result = await session.execute(query)
#         docs_sales_list = result.fetchall()
#     for doc in docs_sales_list:
#         async with async_session_maker() as session:
#             autorepeat_doc = AutoRepeat(doc=doc, session=session, date_now=date_now)
#             await autorepeat_doc.get_last_created_at()
#             await autorepeat_doc.start()


# @scheduler.scheduled_job("interval", seconds=amo_interval, id="amo_import")
# async def amo_import():
#     await database.connect()
#     balances = await database.fetch_all(accounts_balances.select())
#     tariff = await database.fetch_one(tariffs.select().where(tariffs.c.actual == True))
#     for balance in balances:
#         if balance.tariff_type == DEMO:
#             now = datetime.utcnow()
#             if now >= datetime.fromtimestamp(balance.created_at) + timedelta(
#                     days=tariff.demo_days
#             ):
#                 await make_account(balance)
#         elif balance.tariff_type == PAID:
#             await make_account(balance)


# @scheduler.scheduled_job("interval", seconds=5, id="repeat_payments")
# async def repeat_payments():
#     await database.connect()
#     query = payments.select().filter(payments.c.repeat_period != None)
#     payments_db = await database.fetch_all(query)
#     for payment in payments_db:
#         child_payments_query = (
#             payments.select()
#             .filter(payments.c.repeat_parent_id == payment.id)
#             .order_by(desc(payments.c.created_at))
#         )
#         child_payments = await database.fetch_all(child_payments_query)
#         last_payment = payment if not child_payments else child_payments[0]
#         last_time = datetime.fromtimestamp(last_payment.created_at)
#         now = datetime.utcnow()
#         if payment.repeat_number and len(child_payments) >= payment.repeat_number:
#             await clear_repeats(payment.id)
#             continue
#         if payment.repeat_first:
#             if payment.repeat_first > now.timestamp():
#                 continue
#             elif last_time.timestamp() < payment.repeat_first <= now.timestamp():
#                 await repeat_payment(last_payment, payment.id)
#                 continue
#         # If the first_repeat already done or no first_repeat:
#         if payment.repeat_period == RepeatPeriod.YEARLY:
#             if not payment.repeat_month or not payment.repeat_day:
#                 continue
#             if now.day == payment.repeat_day and now.month == payment.repeat_month:
#                 if now - timedelta(days=1) >= last_time:
#                     await repeat_payment(last_payment, payment.id)
#         elif payment.repeat_period == RepeatPeriod.MONTHLY:
#             if not payment.repeat_day:
#                 continue
#             if now.day == payment.repeat_day:
#                 if now - timedelta(days=1) >= last_time:
#                     await repeat_payment(last_payment, payment.id)
#         elif payment.repeat_period == RepeatPeriod.WEEKLY:
#             if not payment.repeat_weekday:
#                 continue
#             try:
#                 payment_weekdays = [
#                     *map(
#                         lambda x: int(x) if x else None,
#                         payment.repeat_weekday.split(","),
#                     )
#                 ]
#             except ValueError as e:
#                 print("Error in payment_weekdays:", e)
#                 continue
#             if now.weekday in payment_weekdays:
#                 if now - timedelta(days=1) >= last_time:
#                     await repeat_payment(last_payment, payment.id)
#         elif payment.repeat_period == RepeatPeriod.DAILY:
#             if now - timedelta(days=1) >= last_time:
#                 await repeat_payment(last_payment, payment.id)
#         elif payment.repeat_period == RepeatPeriod.HOURLY:
#             if now - timedelta(hours=1) >= last_time:
#                 await repeat_payment(last_payment, payment.id)
#         elif payment.repeat_period == RepeatPeriod.SECONDS:
#             if not payment.repeat_seconds:
#                 continue
#             if now - timedelta(seconds=payment.repeat_seconds) >= last_time:
#                 await repeat_payment(last_payment, payment.id)
#
#         if payment.repeat_last and payment.repeat_last < now.timestamp():
#             await clear_repeats(payment.id)


# @scheduler.scheduled_job("interval", seconds=5, id="distribution")
# async def distribution():
#     await database.connect()
#     await process_distribution()
#     await process_gross_profit_report()







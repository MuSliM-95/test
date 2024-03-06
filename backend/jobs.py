import os
from datetime import datetime, timedelta
from time import sleep

import aiohttp
from apscheduler.jobstores.base import JobLookupError
from fastapi.exceptions import HTTPException
from sqlalchemy import desc, select, or_, and_, alias
from sqlalchemy.exc import DatabaseError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from pytz import utc

from api.contracts.schemas import PaymentType
from api.payments.routers import read_payments_list, create_payment
from api.payments.schemas import PaymentCreate
from apps.tochka_bank.schemas import StatementData

from const import PAID, DEMO, RepeatPeriod
from database.db import engine, accounts_balances, database, tariffs, payments, loyality_transactions, loyality_cards,\
    cboxes, engine_job_store, tochka_bank_accounts, tochka_bank_credentials, pboxes, users_cboxes_relation,\
    entity_to_entity, tochka_bank_payments
from functions.account import make_account
from functions.filter_schemas import PaymentFiltersQuery
from functions.goods_distribution import process_distribution
from functions.gross_profit import process_gross_profit_report
from functions.payments import clear_repeats, repeat_payment

scheduler = AsyncIOScheduler(
    {"apscheduler.job_defaults.max_instances": 25}, timezone=utc
)
jobstore = SQLAlchemyJobStore(engine=engine_job_store)
try:
    try:
        jobstore.remove_job("check_account")
    except JobLookupError:
        pass
    try:
        jobstore.remove_job("autoburn")
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
    # try:
        # jobstore.remove_job("amo_import")
    # except JobLookupError:
    #     pass
except DatabaseError:
    pass

scheduler.add_jobstore(jobstore)


def add_job_to_sched(func, **kwargs):
    scheduler.add_job(func, **kwargs)


accountant_interval = int(os.getenv("ACCOUNT_INTERVAL", default=300))


async def get_statement(statement_id: str, account_id: str, access_token: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://enter.tochka.com/uapi/open-banking/v1.0/accounts/{account_id}/statements/{statement_id}',
                               headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'}) as resp:
            try:
                init_statement_json = await resp.json()
            except:
                init_statement_json = {"Data": {"Statement": {"status": "error"}}}
        await session.close()
    return init_statement_json


async def init_statement(statement_data: dict, access_token: str):
    async with aiohttp.ClientSession() as session:
        async with session.post(f'https://enter.tochka.com/uapi/open-banking/v1.0/statements', json = {
            'Data': {
                'Statement': {
                    'accountId': statement_data.get('accountId'),
                    'startDateTime': statement_data.get('startDateTime'),
                    'endDateTime': statement_data.get('endDateTime'),
                }
            }
        }, headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'}) as resp:
            try:
                init_statement_json = await resp.json()
            except:
                init_statement_json = {"Data": {"Statement": {"status": "error"}}}
        await session.close()
    return init_statement_json



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


# @scheduler.scheduled_job("interval", seconds=5, id="autoburn")
# async def autoburn():
#     await database.connect()
#
#     all_cards = await database.fetch_all(loyality_cards.select().where(loyality_cards.c.balance > 0))
#     for card in all_cards:
#         card_id = card.id
#         balance = card.balance
#         lifetime = card.lifetime
#         if lifetime:
#             q = loyality_transactions.select().where(loyality_transactions.c.loyality_card_id == card_id)
#             all_transactions = await database.fetch_all(q)
#             total_accrual = 0
#             for transaction in all_transactions:
#                 if transaction.created_at.timestamp() + lifetime < datetime.now().timestamp():
#                     if transaction.type == "accrual":
#                         total_accrual += transaction.amount
#             burn_amount = balance - total_accrual
#             if burn_amount > 0:
#                 new_balance = balance - burn_amount
#                 query = loyality_cards.update().where(loyality_cards.c.id == card_id).values({"balance": new_balance})
#                 await database.execute(query)
#
#             cashbox = await database.fetch_one(cboxes.select().where(cboxes.c.id == card.cashbox_id))
#             admin = cashbox.admin
#
#             rubles_body = {
#                 "type": "autoburned",
#                 "dated": datetime.now(),
#                 "amount": burn_amount,
#                 "loyality_card_id": card_id,
#                 "loyality_card_number": card.card_number,
#                 "created_by_id": admin,
#                 "cashbox": cashbox['id'],
#                 "tags": "",
#                 "name": "Автосписание",
#                 "description": None,
#                 "status": True,
#                 "external_id": None,
#                 "cashier_name": None,
#                 # "percentamount": None, что это?
#                 # "preamount": None, что это?
#                 "dead_at": None,
#                 "is_deleted": False,
#                 "autoburned": True,
#                 "created_at": datetime.now(),
#                 "updated_at": datetime.now()
#             }
#
#             await database.execute(loyality_transactions.insert().values(rubles_body))



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


@scheduler.scheduled_job('interval', minutes=2, id="tochka_update_transaction")
async def tochka_update_transaction():
    await database.connect()
    active_accounts_with_credentials = await database.fetch_all(
        select(tochka_bank_accounts.c.accountId,
               tochka_bank_accounts.c.registrationDate,
               tochka_bank_credentials.c.access_token,
               pboxes.c.id.label("pbox_id"),
               users_cboxes_relation.c.token,
               pboxes.c.cashbox.label("cashbox_id")
               ).
        where(
            and_(
                tochka_bank_accounts.c.is_active == True,
                tochka_bank_accounts.c.is_deleted == False
            )
        ).
        select_from(tochka_bank_accounts).
        join(tochka_bank_credentials, tochka_bank_credentials.c.id == tochka_bank_accounts.c.tochka_bank_credential_id).
        join(pboxes, pboxes.c.id == tochka_bank_accounts.c.payboxes_id).
        join(users_cboxes_relation, users_cboxes_relation.c.id == pboxes.c.cashbox)
    )
    if active_accounts_with_credentials:
        for account in active_accounts_with_credentials:
            statement = await init_statement({
                    "accountId": account.get('accountId'),
                    "startDateTime": account.get('registrationDate'),
                    "endDateTime": str(datetime.utcnow().date())
                }, account.get('access_token'))
            status_info = ''
            info_statement = None
            print(statement)
            while status_info != 'Ready':
                sleep(2)
                info_statement = await get_statement(
                    statement.get('Data')['Statement'].get('statementId'),
                    statement.get('Data')['Statement'].get('accountId'),
                    account.get('access_token'))
                status_info = info_statement.get('Data')['Statement'][0].get('status')

            tochka_payments_db = await database.fetch_all(
                select(*payments.columns, tochka_bank_payments.c.paymentId).
                where(and_(payments.c.paybox == account.get('pbox_id'), payments.c.cashbox == account.get('cashbox_id'))).\
                select_from(payments).\
                join(tochka_bank_payments, tochka_bank_payments.c.payment_crm_id == payments.c.id))

            if len(tochka_payments_db) < 1:
                for payment in info_statement.get('Data')['Statement'][0]['Transaction']:
                    print(payment)
                    payment_create = await create_payment(account.get('token'), PaymentCreate(
                        name = payment.get('transactionTypeCode'),
                        description = payment.get('description'),
                        type = 'incoming' if payment.get('creditDebitIndicator') == 'Debit' else 'outgoing',
                        tags = f"TochkaBank,{account.get('accountId')}",
                        amount = payment.get('Amount').get('amount'),
                        paybox = account.get('pbox_id'),
                        amount_without_tax = payment.get('Amount').get('amount'),
                        status = True if payment.get('status') == 'Booked' else False,
                        stopped = True
                    ))

                    payment_data = {
                        'accountId': info_statement.get('accountId'),
                        'payment_crm_id': payment_create.get('id'),
                        'statementId': info_statement.get('statementId'),
                        'statement_creation_datetime': info_statement.get('creationDateTime'),
                        'transactionTypeCode': payment.get('transactionTypeCode'),
                        'transactionId': payment.get('transactionId'),
                        'status': payment.get('status'),
                        'paymentId': payment.get('paymentId'),
                        'documentProcessDate': payment.get('documentProcessDate'),
                        'documentNumber': payment.get('documentNumber'),
                        'description': payment.get('description'),
                        'creditDebitIndicator': payment.get('creditDebitIndicator'),
                        'amount': payment.get('Amount').get('amount') if payment.get('Amount') else None,
                        'amountNat': payment.get('Amount').get('amountNat') if payment.get('Amount') else None,
                        'currency': payment.get('Amount').get('currency') if payment.get('Amount') else None,
                    }
                    if payment.get('CreditorParty'):
                        payment_data.update({
                            'creditor_party_inn': payment.get('CreditorParty').get('inn'),
                            'creditor_party_name': payment.get('CreditorParty').get('name'),
                            'creditor_party_kpp': payment.get('CreditorParty').get('kpp'),
                            'creditor_account_identification': payment.get('CreditorAccount').get('identification'),
                            'creditor_account_schemeName': payment.get('CreditorAccount').get('schemeName'),
                            'creditor_agent_schemeName': payment.get('CreditorAgent').get('schemeName'),
                            'creditor_agent_name': payment.get('CreditorAgent').get('name'),
                            'creditor_agent_identification': payment.get('CreditorAgent').get('identification'),
                            'creditor_agent_accountIdentification': payment.get('CreditorAgent').get('accountIdentification'),
                        })
                    elif payment.get('DebtorParty'):
                        payment_data.update({
                            'debitor_party_inn': payment.get('DebtorParty').get('inn'),
                            'debitor_party_name': payment.get('DebtorParty').get('name'),
                            'debitor_party_kpp': payment.get('DebtorParty').get('kpp'),
                            'debitor_account_identification': payment.get('DebtorAccount').get('identification'),
                            'debitor_account_schemeName': payment.get('DebtorAccount').get('schemeName'),
                            'debitor_agent_schemeName': payment.get('DebtorAgent').get('schemeName'),
                            'debitor_agent_name': payment.get( 'DebtorAgent').get('name'),
                            'debitor_agent_identification': payment.get('DebtorAgent').get('identification'),
                            'debitor_agent_accountIdentification': payment.get('DebtorAgent').get('accountIdentification'),
                        }),
                    else:
                        raise Exception('не вилидный формат транзакции от Точка банка')

                    await database.execute(tochka_bank_payments.insert().values(payment_data))
            else:
                set_tochka_payments_statement = set([item.get('paymentId') for item in info_statement.get('Data')['Statement'][0]['Transaction']])
                set_tochka_payments_db = set([item.get('paymentId') for item in tochka_payments_db])
                new_paymentsId = list(set_tochka_payments_statement - set_tochka_payments_db)
                for payment in [item for item in info_statement.get('Data')['Statement'][0]['Transaction'] if item.get('paymentId') in new_paymentsId]:
                    payment_create = await create_payment(account.get('token'), PaymentCreate(
                        name = payment.get('transactionTypeCode'),
                        description = payment.get('description'),
                        type = 'incoming' if payment.get('creditDebitIndicator') == 'Debit' else 'outgoing',
                        tags = f"TochkaBank,{account.get('accountId')}",
                        amount = payment.get('Amount').get('amount'),
                        paybox = account.get('pbox_id'),
                        amount_without_tax = payment.get('Amount').get('amount'),
                        status = True if payment.get('status') == 'Booked' else False,
                        stopped = True
                    ))
                    payment_data = {
                        'accountId': info_statement.get('accountId'),
                        'payment_crm_id': payment_create.get('id'),
                        'statementId': info_statement.get('statementId'),
                        'statement_creation_datetime': info_statement.get('creationDateTime'),
                        'transactionTypeCode': payment.get('transactionTypeCode'),
                        'transactionId': payment.get('transactionId'),
                        'status': payment.get('status'),
                        'paymentId': payment.get('paymentId'),
                        'documentProcessDate': payment.get('documentProcessDate'),
                        'documentNumber': payment.get('documentNumber'),
                        'description': payment.get('description'),
                        'creditDebitIndicator': payment.get('creditDebitIndicator'),
                        'amount': payment.get('Amount').get('amount') if payment.get('Amount') else None,
                        'amountNat': payment.get('Amount').get('amountNat') if payment.get('Amount') else None,
                        'currency': payment.get('Amount').get('currency') if payment.get('Amount') else None,
                    }
                    if payment.get('CreditorParty'):
                        payment_data.update({
                            'creditor_party_inn': payment.get('CreditorParty').get('inn'),
                            'creditor_party_name': payment.get('CreditorParty').get('name'),
                            'creditor_party_kpp': payment.get('CreditorParty').get('kpp'),
                            'creditor_account_identification': payment.get('CreditorAccount').get('identification'),
                            'creditor_account_schemeName': payment.get('CreditorAccount').get('schemeName'),
                            'creditor_agent_schemeName': payment.get('CreditorAgent').get('schemeName'),
                            'creditor_agent_name': payment.get('CreditorAgent').get('name'),
                            'creditor_agent_identification': payment.get('CreditorAgent').get('identification'),
                            'creditor_agent_accountIdentification': payment.get('CreditorAgent').get('accountIdentification'),
                        })
                    elif payment.get('DebtorParty'):
                        payment_data.update({
                            'debitor_party_inn': payment.get('DebtorParty').get('inn'),
                            'debitor_party_name': payment.get('DebtorParty').get('name'),
                            'debitor_party_kpp': payment.get('DebtorParty').get('kpp'),
                            'debitor_account_identification': payment.get('DebtorAccount').get('identification'),
                            'debitor_account_schemeName': payment.get('DebtorAccount').get('schemeName'),
                            'debitor_agent_schemeName': payment.get('DebtorAgent').get('schemeName'),
                            'debitor_agent_name': payment.get('DebtorAgent').get('name'),
                            'debitor_agent_identification': payment.get('DebtorAgent').get('identification'),
                            'debitor_agent_accountIdentification': payment.get('DebtorAgent').get('accountIdentification'),
                        }),
                    else:
                        raise Exception( 'не вилидный формат транзакции от Точка банка' )

                    await database.execute( tochka_bank_payments.insert().values(payment_data))

                for payment in info_statement.get('Data')['Statement'][0]['Transaction']:
                    payment_data = {
                        'accountId': info_statement.get('accountId'),
                        'statementId': info_statement.get('statementId'),
                        'statement_creation_datetime': info_statement.get('creationDateTime'),
                        'transactionTypeCode': payment.get('transactionTypeCode'),
                        'transactionId': payment.get('transactionId'),
                        'status': payment.get('status'),
                        'paymentId': payment.get('paymentId'),
                        'documentProcessDate': payment.get('documentProcessDate'),
                        'documentNumber': payment.get('documentNumber'),
                        'description': payment.get('description'),
                        'creditDebitIndicator': payment.get('creditDebitIndicator'),
                        'amount': payment.get('Amount').get('amount') if payment.get('Amount') else None,
                        'amountNat': payment.get('Amount').get('amountNat') if payment.get('Amount') else None,
                        'currency': payment.get('Amount').get('currency') if payment.get('Amount') else None,
                    }
                    print(payment)
                    if payment.get('CreditorParty'):
                        payment_data.update({
                            'creditor_party_inn': payment.get('CreditorParty').get('inn'),
                            'creditor_party_name': payment.get('CreditorParty').get('name'),
                            'creditor_party_kpp': payment.get('CreditorParty').get('kpp'),
                            'creditor_account_identification': payment.get('CreditorAccount').get('identification'),
                            'creditor_account_schemeName': payment.get('CreditorAccount').get('schemeName'),
                            'creditor_agent_schemeName': payment.get('CreditorAgent').get('schemeName'),
                            'creditor_agent_name': payment.get('CreditorAgent').get('name'),
                            'creditor_agent_identification': payment.get('CreditorAgent').get('identification'),
                            'creditor_agent_accountIdentification': payment.get('CreditorAgent').get('accountIdentification'),
                        })
                    elif payment.get('DebtorParty'):
                        payment_data.update({
                            'debitor_party_inn': payment.get('DebtorParty').get('inn'),
                            'debitor_party_name': payment.get('DebtorParty').get('name'),
                            'debitor_party_kpp': payment.get('DebtorParty').get('kpp'),
                            'debitor_account_identification': payment.get('DebtorAccount').get('identification'),
                            'debitor_account_schemeName': payment.get('DebtorAccount').get('schemeName'),
                            'debitor_agent_schemeName': payment.get('DebtorAgent').get('schemeName'),
                            'debitor_agent_name': payment.get('DebtorAgent').get('name'),
                            'debitor_agent_identification': payment.get('DebtorAgent').get('identification'),
                            'debitor_agent_accountIdentification': payment.get('DebtorAgent').get('accountIdentification'),
                        }),
                    else:
                        raise Exception('не вилидный формат транзакции от Точка банка')

                    await database.execute(tochka_bank_payments.update().where(tochka_bank_payments.c.paymentId == payment.get('paymentId')).values(payment_data))

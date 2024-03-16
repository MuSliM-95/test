import os
from datetime import datetime, timedelta
from time import sleep
from typing import List, Union, Any, Dict
from itertools import zip_longest

import aiohttp
from apscheduler.jobstores.base import JobLookupError
from databases.backends.postgres import Record
from fastapi.exceptions import HTTPException
from sqlalchemy import desc, select, or_, and_, alias, func, asc
from sqlalchemy.exc import DatabaseError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from pytz import utc

from api.contracts.schemas import PaymentType
from api.payments.routers import read_payments_list, create_payment
from api.payments.schemas import PaymentCreate
from apps.tochka_bank.schemas import StatementData

from const import PAID, DEMO, RepeatPeriod
from database.db import engine, accounts_balances, database, tariffs, payments, loyality_transactions, loyality_cards, \
    cboxes, engine_job_store, tochka_bank_accounts, tochka_bank_credentials, pboxes, users_cboxes_relation, \
    entity_to_entity, tochka_bank_payments, contragents
from functions.account import make_account
from functions.filter_schemas import PaymentFiltersQuery
from functions.goods_distribution import process_distribution
from functions.gross_profit import process_gross_profit_report
from functions.helpers import init_statement, get_statement
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


@scheduler.scheduled_job("interval", seconds=5, id="autoburn")
async def autoburn():
    await database.connect()

    class AutoBurn:
        def __init__(self, card: Record) -> None:
            self.card: Record = card
            self.card_balance: float = card.balance
            self.first_operation_burned: Union[int, None] = None
            self.accrual_list: List[dict] = []
            self.withdraw_list: List[dict] = []
            self.burned_list: List[int] = []
            self.autoburn_operation_list: List[dict] = []

        @staticmethod
        async def get_cards() -> List[Record]:
            cards_query = (
                loyality_cards
                .select()
                .where(
                    loyality_cards.c.balance > 0,
                    loyality_cards.c.lifetime.is_not(None),
                    loyality_cards.c.lifetime > 0
                )
            )
            return await database.fetch_all(cards_query)

        async def _get_first_operation_burned(self) -> None:
            q_first = (
                select(loyality_transactions.c.id)
                .where(
                    loyality_transactions.c.loyality_card_id == self.card.id,
                    loyality_transactions.c.type == "accrual",
                    loyality_transactions.c.amount > 0,
                    loyality_transactions.c.autoburned.is_not(True),
                    loyality_transactions.c.created_at + timedelta(seconds=self.card.lifetime) < datetime.now(),
                    loyality_transactions.c.card_balance == 0
                )
                .order_by(asc(loyality_transactions.c.id))
                .limit(1)
            )
            self.first_operation_burned = await database.fetch_val(q_first)

        async def _get_transaction(self) -> None:
            if self.first_operation_burned is not None:
                q = (
                    loyality_transactions
                    .select()
                    .where(
                        loyality_transactions.c.loyality_card_id == self.card.id,
                        loyality_transactions.c.type.in_(["accrual", "withdraw"]),
                        loyality_transactions.c.amount > 0,
                        loyality_transactions.c.autoburned.is_not(True),
                        loyality_transactions.c.id >= self.first_operation_burned
                    )
                )
                transaction_list = await database.fetch_all(q)

                minus_index = 0
                self.accrual_list.extend([dict(i) for i in transaction_list if i.type == "accrual"])
                for transaction in transaction_list:
                    transaction: Dict[str, Any] = dict(transaction)
                    self.burned_list.append(transaction["id"])
                    if transaction["type"] == "withdraw":
                        if self.accrual_list[minus_index]["amount"] > 0:
                            if self.accrual_list[minus_index]["amount"] >= transaction["amount"]:
                                self.accrual_list[minus_index]["amount"] -= transaction["amount"]
                            else:
                                transaction["amount"] = transaction["amount"] - self.accrual_list[minus_index]["amount"]
                                self.accrual_list[minus_index]["amount"] = 0
                            if self.accrual_list[minus_index]["amount"] == 0:
                                minus_index += 1

                        self.withdraw_list.append(transaction)

        @database.transaction()
        async def _burn(self) -> None:
            update_transaction_status_query = (
                loyality_transactions
                .update()
                .where(
                    loyality_transactions.c.id.in_(self.burned_list)
                )
                .values({"autoburned": True})
            )
            await database.execute(update_transaction_status_query)

            update_balance_query = (
                loyality_cards
                .update()
                .where(loyality_cards.c.id == self.card.id)
                .values({"balance": self.card_balance})
            )
            await database.execute(update_balance_query)

            create_transcation_query = (
                loyality_transactions
                .insert()
                .values()
            )
            await database.execute_many(query=create_transcation_query, values=self.autoburn_operation_list)

        def _get_autoburned_operation_dict(
                self,
                update_balance_sum: float,
                amount: float,
                created_at: datetime
        ) -> dict:
            return {
                "type": "autoburned",
                "amount": update_balance_sum,
                "loyality_card_id": self.card.id,
                "loyality_card_number": self.card.card_number,
                "created_by_id": self.card.created_by_id,
                "cashbox": self.card.cashbox_id,
                "tags": "",
                "name": f"Автосгорание от {created_at.strftime('%d.%m.%Y')} по сумме {amount}",
                "description": None,
                "status": True,
                "external_id": None,
                "cashier_name": None,
                "dead_at": None,
                "is_deleted": False,
                "autoburned": True,
                "card_balance": self.card_balance
            }

        async def start(self) -> None:
            await self._get_first_operation_burned()
            await self._get_transaction()
            for a in self.accrual_list:
                amount, update_balance_sum = a["amount"], 0
                if amount == 0:
                    continue

                w = 0
                while w < len(self.withdraw_list):
                    if amount == 0:
                        break

                    if a["amount"] >= self.withdraw_list[w]["amount"]:
                        update_balance_sum += a["amount"] - self.withdraw_list[w]["amount"]
                        del self.withdraw_list[w]
                        w -= 1
                    else:
                        update_balance_sum += a["amount"]
                        self.withdraw_list[w]["amount"] -= a["amount"]
                    amount -= update_balance_sum
                    w += 1

                if update_balance_sum != 0:
                    self.card_balance -= update_balance_sum
                    self.autoburn_operation_list.append(
                        self._get_autoburned_operation_dict(
                            update_balance_sum=update_balance_sum, amount=a["amount"], created_at=a["created_at"]
                        )
                    )
                else:
                    self.card_balance -= a["amount"]
                    self.autoburn_operation_list.append(
                        self._get_autoburned_operation_dict(
                            update_balance_sum=a["amount"], amount=a["amount"], created_at=a["created_at"]
                        )
                    )

            await self._burn()

    card_list = await AutoBurn.get_cards()
    for card in card_list:
        await AutoBurn(card=card).start()


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


@scheduler.scheduled_job('interval', minutes=5, id="tochka_update_transaction")
async def tochka_update_transaction():
    await database.connect()

    @database.transaction()
    async def _tochka_update():
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
            join(tochka_bank_credentials,
                 tochka_bank_credentials.c.id == tochka_bank_accounts.c.tochka_bank_credential_id).
            join(pboxes, pboxes.c.id == tochka_bank_accounts.c.payboxes_id).
            join(users_cboxes_relation, users_cboxes_relation.c.id == pboxes.c.cashbox)
        )
        if active_accounts_with_credentials:
            for account in active_accounts_with_credentials:
                async with aiohttp.ClientSession(trust_env=True) as session:
                    async with session.get(
                            f'https://enter.tochka.com/uapi/open-banking/v1.0/accounts/{account.get("accountId")}/balances',
                            headers={
                                'Authorization': f'Bearer {account.get("access_token")}',
                                'Content-type': 'application/json'
                            }) as resp:
                        balance_json = await resp.json()
                    await session.close()

                if not balance_json.get("Data"):
                    raise Exception("проблема с получением баланса (вероятно некорректный access_token)")

                await database.execute(pboxes.
                update().
                where(pboxes.c.id == account.get('pbox_id')).
                values(
                    {
                        'balance': balance_json.get("Data").get("Balance")[0].get("Amount").get("amount"),
                        'updated_at': int(datetime.utcnow().timestamp()),
                        'balance_date': int(datetime.utcnow().timestamp())
                    }
                ))

                statement = await init_statement({
                    "accountId": account.get('accountId'),
                    "startDateTime": account.get('registrationDate'),
                    "endDateTime": str(datetime.now().date() + timedelta(days=1))
                }, account.get('access_token'))
                status_info = ''
                info_statement = None
                while status_info != 'Ready':
                    sleep(2)
                    info_statement = await get_statement(
                        statement.get('Data')['Statement'].get('statementId'),
                        statement.get('Data')['Statement'].get('accountId'),
                        account.get('access_token'))
                    status_info = info_statement.get('Data')['Statement'][0].get('status')
                tochka_payments_db = await database.fetch_all(
                    select(*payments.columns, tochka_bank_payments.c.paymentId).
                    where(and_(payments.c.paybox == account.get('pbox_id'),
                               payments.c.cashbox == account.get('cashbox_id'))). \
                    select_from(payments). \
                    join(tochka_bank_payments, tochka_bank_payments.c.payment_crm_id == payments.c.id))

                if len(tochka_payments_db) < 1:
                    for payment in info_statement.get('Data')['Statement'][0]['Transaction']:
                        payment_create_id = await database.execute(payments.insert().values({
                            'name': payment.get('transactionTypeCode'),
                            'description': payment.get('description'),
                            'type': 'outgoing' if payment.get('creditDebitIndicator') == 'Debit' else 'incoming',
                            'tags': f"TochkaBank,{account.get('accountId')}",
                            'amount': payment.get('Amount').get('amount'),
                            'cashbox': account.get('cashbox_id'),
                            'paybox': account.get('pbox_id'),
                            'date': datetime.strptime(payment.get('documentProcessDate'), "%Y-%m-%d").timestamp(),
                            'created_at': int(datetime.utcnow().timestamp()),
                            'updated_at': int(datetime.utcnow().timestamp()),
                            'is_deleted': False,
                            'amount_without_tax': payment.get('Amount').get('amount'),
                            'status': True if payment.get('status') == 'Booked' else False,
                            'stopped': True
                        }))
                        payment_create = await database.fetch_one(
                            payments.select().where(payments.c.id == payment_create_id))
                        payment_data = {
                            'accountId': info_statement.get('Data')['Statement'][0].get('accountId'),
                            'payment_crm_id': payment_create.get('id'),
                            'statementId': info_statement.get('Data')['Statement'][0].get('statementId'),
                            'statement_creation_datetime': info_statement.get('Data')['Statement'][0].get(
                                'creationDateTime'),
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
                                'creditor_agent_accountIdentification': payment.get('CreditorAgent').get(
                                    'accountIdentification'),
                            })

                            contragent_db = await database.fetch_one(
                                contragents.select().where(
                                    and_(
                                        contragents.c.inn == payment.get('CreditorParty').get('inn')),
                                    contragents.c.cashbox == account.get('cashbox_id')
                                ))
                            if not contragent_db:
                                contragent_db = await database.execute(contragents.insert().values({
                                    'name': payment.get('CreditorParty').get('name'),
                                    'inn': payment.get('CreditorParty').get('inn'),
                                    'cashbox': account.get('cashbox_id'),
                                    'is_deleted': False,
                                    'created_at': int(datetime.utcnow().timestamp()),
                                    'updated_at': int(datetime.utcnow().timestamp()),
                                }))
                                await database.execute(
                                    payments.update().where(payments.c.id == payment_create.get('id')).values(
                                        {'contragent': contragent_db}))
                            else:
                                await database.execute(
                                    payments.update().where(payments.c.id == payment_create.get('id')).values(
                                        {'contragent': contragent_db.get('id')}))

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
                                'debitor_agent_accountIdentification': payment.get('DebtorAgent').get(
                                    'accountIdentification'),
                            }),

                            contragent_db = await database.fetch_one(
                                contragents.select().where(and_(
                                    contragents.c.inn == payment.get('DebtorParty').get('inn')),
                                    contragents.c.cashbox == account.get('cashbox_id')
                                ))
                            if not contragent_db:
                                contragent_db = await database.execute(contragents.insert().values({
                                    'name': payment.get('DebtorParty').get('name'),
                                    'inn': payment.get('DebtorParty').get('inn'),
                                    'cashbox': account.get('cashbox_id'),
                                    'is_deleted': False,
                                    'created_at': int(datetime.utcnow().timestamp()),
                                    'updated_at': int(datetime.utcnow().timestamp()),
                                }))
                                await database.execute(
                                    payments.update().where(payments.c.id == payment_create.get('id')).values(
                                        {'contragent': contragent_db}))
                            else:
                                await database.execute(
                                    payments.update().where(payments.c.id == payment_create.get('id')).values(
                                        {'contragent': contragent_db.get('id')}))
                        else:
                            raise Exception('не вилидный формат транзакции от Точка банка')

                        await database.execute(tochka_bank_payments.insert().values(payment_data))

                else:
                    set_tochka_payments_statement = set(
                        [item.get('paymentId') for item in info_statement.get('Data')['Statement'][0]['Transaction']])
                    set_tochka_payments_db = set([item.get('paymentId') for item in tochka_payments_db])
                    new_paymentsId = list(set_tochka_payments_statement - set_tochka_payments_db)
                    for payment in [item for item in info_statement.get('Data')['Statement'][0]['Transaction'] if
                                    item.get('paymentId') in new_paymentsId]:
                        payment_create_id = await database.execute(payments.insert().values({
                            'name': payment.get('transactionTypeCode'),
                            'description': payment.get('description'),
                            'type': 'outgoing' if payment.get('creditDebitIndicator') == 'Debit' else 'incoming',
                            'tags': f"TochkaBank,{account.get('accountId')}",
                            'amount': payment.get('Amount').get('amount'),
                            'cashbox': account.get('cashbox_id'),
                            'paybox': account.get('pbox_id'),
                            'date': datetime.strptime(payment.get('documentProcessDate'), "%Y-%m-%d").timestamp(),
                            'created_at': int(datetime.utcnow().timestamp()),
                            'updated_at': int(datetime.utcnow().timestamp()),
                            'is_deleted': False,
                            'amount_without_tax': payment.get('Amount').get('amount'),
                            'status': True if payment.get('status') == 'Booked' else False,
                            'stopped': True
                        }))
                        payment_create = await database.fetch_one(
                            payments.select().where(payments.c.id == payment_create_id))
                        payment_data = {
                            'accountId': info_statement.get('Data')['Statement'][0].get('accountId'),
                            'payment_crm_id': payment_create.get('id'),
                            'statementId': info_statement.get('Data')['Statement'][0].get('statementId'),
                            'statement_creation_datetime': info_statement.get('Data')['Statement'][0].get(
                                'creationDateTime'),
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
                                'creditor_agent_accountIdentification': payment.get('CreditorAgent').get(
                                    'accountIdentification'),
                            })

                            contragent_db = await database.fetch_one(
                                contragents.select().where(and_(
                                    contragents.c.inn == payment.get('CreditorParty').get('inn')),
                                    contragents.c.cashbox == account.get('cashbox_id')
                                ))
                            if not contragent_db:
                                contragent_db = await database.execute(contragents.insert().values({
                                    'name': payment.get('CreditorParty').get('name'),
                                    'inn': payment.get('CreditorParty').get('inn'),
                                    'cashbox': account.get('cashbox_id'),
                                    'is_deleted': False,
                                    'created_at': int(datetime.utcnow().timestamp()),
                                    'updated_at': int(datetime.utcnow().timestamp()),
                                }))
                                await database.execute(
                                    payments.update().where(payments.c.id == payment_create.get('id')).values(
                                        {'contragent': contragent_db}))
                            else:
                                await database.execute(
                                    payments.update().where(payments.c.id == payment_create.get('id')).values(
                                        {'contragent': contragent_db.get('id')}))

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
                                'debitor_agent_accountIdentification': payment.get('DebtorAgent').get(
                                    'accountIdentification'),
                            })
                            contragent_db = await database.fetch_one(
                                contragents.select().where(and_(
                                    contragents.c.inn == payment.get('DebtorParty').get('inn')),
                                    contragents.c.cashbox == account.get('cashbox_id')
                                ))
                            if not contragent_db:
                                contragent_db = await database.execute(contragents.insert().values({
                                    'name': payment.get('DebtorParty').get('name'),
                                    'inn': payment.get('DebtorParty').get('inn'),
                                    'cashbox': account.get('cashbox_id'),
                                    'is_deleted': False,
                                    'created_at': int(datetime.utcnow().timestamp()),
                                    'updated_at': int(datetime.utcnow().timestamp()),
                                }))
                                await database.execute(
                                    payments.update().where(payments.c.id == payment_create.get('id')).values(
                                        {'contragent': contragent_db}))
                            else:
                                await database.execute(
                                    payments.update().where(payments.c.id == payment_create.get('id')).values(
                                        {'contragent': contragent_db.get('id')}))
                        else:
                            raise Exception('не вилидный формат транзакции от Точка банка')

                        await database.execute(tochka_bank_payments.insert().values(payment_data))

                    for payment in info_statement.get('Data')['Statement'][0]['Transaction']:
                        payment_data = {
                            'accountId': info_statement.get('Data')['Statement'][0].get('accountId'),
                            'statementId': info_statement.get('Data')['Statement'][0].get('statementId'),
                            'statement_creation_datetime': info_statement.get('Data')['Statement'][0].get(
                                'creationDateTime'),
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
                                'creditor_agent_accountIdentification': payment.get('CreditorAgent').get(
                                    'accountIdentification'),
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
                                'debitor_agent_accountIdentification': payment.get('DebtorAgent').get(
                                    'accountIdentification'),
                            }),
                        else:
                            raise Exception('не вилидный формат транзакции от Точка банка')

                        await database.execute(tochka_bank_payments.update().where(
                            tochka_bank_payments.c.paymentId == payment.get('paymentId')).values(payment_data))
                        payment_update = await database.fetch_one(tochka_bank_payments.select().where(
                            tochka_bank_payments.c.paymentId == payment.get('paymentId')))
                        await database.execute(
                            payments.update().where(payments.c.id == payment_update.get('payment_crm_id')).values({
                                'name': payment.get('transactionTypeCode'),
                                'description': payment.get('description'),
                                'type': 'outgoing' if payment.get('creditDebitIndicator') == 'Debit' else 'incoming',
                                'tags': f"TochkaBank,{account.get('accountId')}",
                                'amount': payment.get('Amount').get('amount'),
                                'cashbox': account.get('cashbox_id'),
                                'paybox': account.get('pbox_id'),
                                'date': datetime.strptime(payment.get('documentProcessDate'), "%Y-%m-%d").timestamp(),
                                'updated_at': int(datetime.utcnow().timestamp()),
                                'is_deleted': False,
                                'amount_without_tax': payment.get('Amount').get('amount'),
                                'status': True if payment.get('status') == 'Booked' else False,
                            }))

    await _tochka_update()

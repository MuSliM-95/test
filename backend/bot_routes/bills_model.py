from hmac import new

import queue
from typing import Any, TypedDict, Optional, List
from datetime import datetime
from sqlalchemy import Enum as SQLEnum, select
from database.db import database, bills, bill_approvers, users, integrations_to_cashbox, cboxes,pboxes,  users_cboxes_relation, tochka_bank_accounts, integrations, tochka_bank_credentials
from enum import Enum
from bot_routes.tochka_api import *

import logging


class BillStatus(str, Enum):
    new = "new"
    waiting_for_approval = "waiting_for_approval"
    approved = "approved"
    canceled = "canceled"
    rejected = "rejected"
    requested = "requested"
    paid = "paid"
    error = "error"

class BillApproveStatus(str, Enum):
    new = "new"
    approved = "approved"
    canceled = "canceled"

class CreateBillData(TypedDict):
    payment_date: Optional[datetime]
    created_by: int
    s3_url: str
    plain_text: str
    file_name: str
    status: BillStatus
    tochka_bank_account_id: Optional[int]
    amount: float
    pc: str
    reason: str
    bic: str
    seller: str
    inn_seller: str
    inn_buyer: str
    corr_account: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime]

class UpdateBillData(TypedDict):
    payment_date: Optional[datetime]
    created_by: Optional[int]
    s3_url: Optional[str]
    plain_text: Optional[str]
    file_name: Optional[str]
    amount: Optional[float]
    pc: Optional[str]
    reason: Optional[str]
    bic: Optional[str]
    seller: Optional[str]
    inn_seller: Optional[str]
    inn_buyer: Optional[str]
    corr_account: Optional[str]
    tochka_bank_account_id: Optional[int]
    status: Optional[BillStatus]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime]

class BillApproverData(TypedDict):
    approver_id: int
    bill_id: int
    status: BillApproveStatus
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime]

class CreateBillApproverData(TypedDict):
    approver_id: int
    bill_id: int
    status: BillApproveStatus




class UpdateBillApproverData(TypedDict):
    bill_id: int
    status: BillApproveStatus



async def update_bill_status_based_on_approvals(bill_id: int):
    """
    Updates the bill status based on the approval statuses and payment date.
    """
    bill = await get_bill(bill_id)
    if not bill:
        logging.warning(f"Bill with id {bill_id} not found.")
        return

    approvers = await get_approvers_by_bill(bill_id)
    if not approvers:
        # If there are no approvers, the bill is considered approved.
        await update_bill_status(bill_id, BillStatus.approved)
        return

    all_approved = all(approver.status == BillApproveStatus.approved for approver in approvers)
    any_canceled = any(approver.status == BillApproveStatus.canceled for approver in approvers)

    if any_canceled:
        await update_bill_status(bill_id, BillStatus.waiting_for_approval)
    elif all_approved:
        await update_bill_status(bill_id, BillStatus.approved)
    else:
        await update_bill_status(bill_id, BillStatus.waiting_for_approval)

async def check_user_permissions(user_id: str, bill_id: int) -> bool:

    bill = await get_bill(bill_id)
    query = users.select().where(users.c.id == bill.created_by)    
    user = await database.fetch_one(query)

    if not bill:
        return False
    if user.chat_id == user_id:
        return True
    
    approvers = await get_approvers_by_bill(bill_id)
    if not approvers:
        return False
    approvers = [approver.approver_id for approver in approvers]
    for approver in approvers:
        if str(approver) == str(user_id):
            return True
    return False


def get_bill_changes(old_bill, new_bill):
    changes = {}
    for key, value in new_bill.items():
        if key in ['created_at', 'updated_at', 'deleted_at']:
            continue
        else:
            if old_bill.get(key, None) != new_bill.get(key, None):
                changes[key] = f"{old_bill.get(key, None)} -> {new_bill.get(key, None)}"
    return changes


async def format_bill_notification(
    
    created_by: int,
    approvers: List[dict],
    updated_by: int,
    new_bill: dict,
    old_bill: dict = {},
) -> str:
    changes = get_bill_changes(old_bill, new_bill)
    message_parts = [f"Создан новый счёт №{new_bill['id']}:"]

    query = users.select().where(users.c.id == created_by)
    user = await database.fetch_one(query)
    query = users.select().where(users.c.chat_id == str(updated_by))
    updated_by = await database.fetch_one(query)
    if old_bill.get('id', None) is  None:
        message_parts = [f"Создан новый счёт №{new_bill.id}:"]
    else:
        message_parts = [f"Счёт №{new_bill.id} был обновлён:"]

    for approver in approvers:
        if approver['status'] == BillApproveStatus.approved:
            message_parts.append(f"  - Одобрен пользователем: {approver['username']}")
        elif approver['status'] == BillApproveStatus.canceled:
            message_parts.append(f"  - Отклонён пользователем: {approver['username']}")
        elif approver['status'] == BillApproveStatus.new:
            message_parts.append(f"  - Необходимо одобрение пользователя: {approver['username']}")
    for key, value in new_bill.items():
        if key in ['created_by','created_at', 'updated_at', 'deleted_at', 'plain_text', 'tochka_bank_account_id']:
            continue
        else:
            message_parts.append(f"  - {key}: {value}")
    if changes:
        message_parts.append("Изменения:")
        for key, value in changes.items():
            message_parts.append(f"  - {key}: {value}")
    message_parts.append(f"  - Создан пользователем: {user.first_name}")
    message_parts.append(f"  - Обновлён пользователем: {updated_by.first_name if updated_by else 'Система'}")

    return "\n".join(message_parts)

async def get_payboxes_by_chat_id(chat_id: str):
    query = users.select().join(users_cboxes_relation, users.c.id == users_cboxes_relation.c.user).join(pboxes, pboxes.c.cashbox == users_cboxes_relation.c.cashbox_id).where(bills.c.chat_id == chat_id)
    return await database.fetch_all(query)

async def get_chat_owner(chat_id: str):
    query = users.select().where(users.c.chat_id == chat_id)
    res =  await database.fetch_one(query)
    query = users.select().where(users.c.owner_id == res["owner_id"])
    res =  await database.fetch_one(query)
    return res


async def send_bill(new_bill, callback_query,  bot):
    if new_bill.status:

        account_arr = new_bill.accountId.split('/')
        try:
            auth = None
            integration = await get_integration_to_cashbox_by_chat_id(str(callback_query.message.chat.id))
            tokens = await get_access_token(new_bill['tochka_bank_account_id'])
            if 'access_token' in tokens:
                auth = tokens['access_token']
            else:
                await update_bill_status(new_bill['id'], BillStatus.error)
                await bot.send_message(chat_id=callback_query.message.chat.id, text="Ошибка при отправке счёта в банк. Не найден токен аccess_token.")
                return
            await bot.send_message(chat_id=callback_query.message.chat.id, text="Отправляю счёт в банк.")
            result = await send_payment_to_tochka(
                account_code=account_arr[0],
                bank_code=account_arr[1],
                counterparty_bank_bic=new_bill.bic,
                counterparty_account_number=new_bill.corr_account,
                counterparty_name=new_bill.seller,
                paymentDate=new_bill.payment_date.strftime("%Y-%m-%d"),
                paymentAmount=new_bill.amount,
                payment_purpose=new_bill.reason,
                auth=auth
            )
            if result["success"]:
                await update_bill(new_bill['id'], { 'requestId': result["request_id"] })
                await update_bill_status(new_bill['id'], BillStatus.requested)
                await bot.send_message(chat_id=callback_query.message.chat.id, text=f"Счет оплачен. запрос: {result['request_id']}")
            else:
                await bot.send_message(chat_id=callback_query.message.chat.id, text="Ошибка при оплате счета.")
                await update_bill_status(new_bill['id'], BillStatus.error)
        except TochkaBankError as e:
            detailed_errors = [f"{err['errorCode']}: {err['message']}" for err in e.errors]
            error_message = f"Bank API Error {e.code}: {e.message}\nDetailed errors: {detailed_errors}"
            await update_bill_status(new_bill['id'], BillStatus.error)
            await bot.send_message(chat_id=callback_query.message.chat.id, text=error_message)
        except Exception as e:
            await update_bill_status(new_bill['id'], BillStatus.error)
            await bot.send_message(chat_id=callback_query.message.chat.id, text=f"Ошибка при отправке счёта в банк. {e}")


async def get_integration_to_cashbox_by_chat_id(chat_id: int):
    chat_owner= await get_chat_owner(chat_id)
    query = (
        select([integrations_to_cashbox])
        .select_from(integrations_to_cashbox)
        .join(integrations, integrations.c.id == integrations_to_cashbox.c.integration_id)
        .join(users_cboxes_relation, users_cboxes_relation.c.id == integrations_to_cashbox.c.installed_by)
        .join(users, users.c.id == users_cboxes_relation.c.user)
        .where( users.c.id == chat_owner["id"], integrations.c.id == 1)

    )
    return await database.fetch_one(query)


async def get_access_token(tochka_bank_accounts_id: int):
    query = (
        select([tochka_bank_credentials])
        .select_from(tochka_bank_credentials)
        .join(tochka_bank_accounts, tochka_bank_accounts.c.tochka_bank_credential_id == tochka_bank_credentials.c.id)
        .where( tochka_bank_accounts.c.id == tochka_bank_accounts_id)

    )
    return await database.fetch_one(query)

async def get_tochka_bank_accounts_by_chat_id(chat_id: str):
    chat_owner= await get_chat_owner(chat_id)
    query = (
        select([tochka_bank_accounts])
        .select_from(tochka_bank_accounts)
        .join(pboxes, tochka_bank_accounts.c.payboxes_id == pboxes.c.id)
        .join(cboxes, pboxes.c.cashbox == cboxes.c.id)
        .join(users, cboxes.c.admin == users.c.id)
        .where( cboxes.c.admin == chat_owner["id"])

    )
    print(query)

    return await database.fetch_all(query)

async def create_bill(bill_data: CreateBillData):
    required_fields = [ "created_by", "s3_url", "file_name", "status", "plain_text"]
    for field in required_fields:
        if field not in bill_data:
            raise ValueError(f"Missing required field: {field}")

    query = bills.insert().values(**bill_data)
    return await database.execute(query)

async def get_bill(bill_id: int):
    query = (
        select([bills, tochka_bank_accounts.c.accountId])
        .select_from(bills)
        .outerjoin(tochka_bank_accounts, bills.c.tochka_bank_account_id == tochka_bank_accounts.c.id)
        .where(bills.c.id == bill_id)
    )
    return await database.fetch_one(query)

async def get_all_bills():
    query = bills.select()
    return await database.fetch_all(query)

async def update_bill(bill_id: int, bill_data: UpdateBillData):
    query = bills.update().where(bills.c.id == bill_id).values(**bill_data)
    return await database.execute(query)

async def update_bill_status(bill_id: int, status: BillStatus):
    query = bills.update().where(bills.c.id == bill_id).values(status=status)
    return await database.execute(query)

async def delete_bill(bill_id: int):
    query = bills.delete().where(bills.c.id == bill_id)
    return await database.execute(query)

async def create_bill_approver(approver_data: CreateBillApproverData):
    required_fields = ["approver_id", "bill_id", "status"]
    for field in required_fields:
        if field not in approver_data:
            raise ValueError(f"Missing required field: {field}")

    query = bill_approvers.insert().values(**approver_data)
    return await database.execute(query)

async def get_bill_approver(approver_id: int):
    query = bill_approvers.select().where(bill_approvers.c.id == approver_id)
    return await database.fetch_one(query)

async def get_approve_by_id_and_approver(approver_id: int, bill_id: int):
    query = bill_approvers.select().where(bill_approvers.c.approver_id == approver_id, bill_approvers.c.bill_id == bill_id)
    return await database.fetch_one(query)

async def get_approvers_by_bill(bill_id: int):
    query = bill_approvers.select().where(bill_approvers.c.bill_id == bill_id)
    return await database.fetch_all(query)

async def update_bill_approve(id: int, approver_data: UpdateBillApproverData):
    query = bill_approvers.update().where(bill_approvers.c.id == id).values(**approver_data)
    return await database.execute(query)

async def delete_bill_approver(approver_id: int):
    query = bill_approvers.delete().where(bill_approvers.c.id == approver_id)
    return await database.execute(query)

async def get_bills_with_approvers():
    query = bills.join(bill_approvers).select()
    return await database.fetch_all(query)
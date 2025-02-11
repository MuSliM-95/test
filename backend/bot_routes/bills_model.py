from hmac import new

from typing import Any, TypedDict, Optional, List
from datetime import datetime
from sqlalchemy import Enum as SQLEnum, select
from database.db import database, bills, bill_approvers, users, pboxes, users_cboxes_relation, tochka_bank_accounts
from enum import Enum

class BillStatus(str, Enum):
    new = "new"
    waiting_for_approval = "waiting_for_approval"
    approved = "approved"
    canceled = "canceled"
    rejected = "rejected"
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


async def get_tochka_bank_accounts_by_chat_id(chat_id: str):
    query = (
        select([tochka_bank_accounts])
        .select_from(tochka_bank_accounts)
        .join(pboxes, tochka_bank_accounts.c.payboxes_id == pboxes.c.id)
        .join(users_cboxes_relation, pboxes.c.cashbox == users_cboxes_relation.c.cashbox_id)
        .join(users, users.c.id == users_cboxes_relation.c.user)
        .where(users.c.chat_id == chat_id)
    )


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
from typing import Any, TypedDict, Optional, List
from datetime import datetime
from sqlalchemy import Enum as SQLEnum
from database.db import database, bills, bill_approvers, users
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
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime]

class UpdateBillData(TypedDict):
    payment_date: Optional[datetime]
    created_by: Optional[int]
    s3_url: Optional[str]
    plain_text: Optional[str]
    file_name: Optional[str]
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


async def format_bill_notification(
    bill_id: int,
    created_by: int,
    s3_url: str,
    file_name: str,
    approvers: List[dict],
    new_payment_date: Optional[datetime],
    new_status: Optional[BillStatus],
    old_payment_date: Optional[datetime] = None,
    old_status: Optional[BillStatus] = None,
    updated_by: Optional[str] = None,
    new_bill: Optional[bool] = False
) -> str:
    
    query = users.select().where(users.c.id == created_by)
    user = await database.fetch_one(query)
    query = users.select().where(users.c.chat_id == str(updated_by))
    updated_by = await database.fetch_one(query)
    if new_bill == True:
        message_parts = [f"Создан новый счёт №{bill_id}:"]
    else:
        message_parts = [f"Счёт №{bill_id} был обновлён:"]

    if old_status and new_status != old_status:
        message_parts.append(f"  - Статус изменён с '{old_status}' на '{new_status}'")

    if new_payment_date and old_payment_date and new_payment_date != old_payment_date:
        message_parts.append(f"  - Дата оплаты изменена с '{old_payment_date.strftime('%Y-%m-%d %H:%M:%S')}' на '{new_payment_date.strftime('%Y-%m-%d %H:%M:%S')}'")

    message_parts.append(f"  - Текущий статус: {new_status}")
    if new_payment_date:
        message_parts.append(f"  - Дата оплаты: {new_payment_date.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        message_parts.append(f"  - Дата оплаты: Не указана")

    for approver in approvers:
        if approver['status'] == BillApproveStatus.approved:
            message_parts.append(f"  - Одобрен пользователем: {approver['username']}")
        elif approver['status'] == BillApproveStatus.canceled:
            message_parts.append(f"  - Отклонён пользователем: {approver['username']}")
        elif approver['status'] == BillApproveStatus.new:
            message_parts.append(f"  - Необходимо одобрение пользователя: {approver['username']}")
    message_parts.append(f"  - Создан пользователем: {user.first_name}")
    message_parts.append(f"  - Обновлён пользователем: {updated_by.first_name if updated_by else 'Система'}")
    message_parts.append(f"  - Файл: {file_name}")
    message_parts.append(f"  - Ссылка: {s3_url}")

    return "\n".join(message_parts)

async def create_bill(bill_data: CreateBillData):
    required_fields = [ "created_by", "s3_url", "file_name", "status", "plain_text"]
    for field in required_fields:
        if field not in bill_data:
            raise ValueError(f"Missing required field: {field}")

    query = bills.insert().values(**bill_data)
    return await database.execute(query)

async def get_bill(bill_id: int):
    query = bills.select().where(bills.c.id == bill_id)
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
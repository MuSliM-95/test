from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, asc, func, select, or_

from const import PaymentType
from ws_manager import manager

from database.db import database, pboxes, users_cboxes_relation, payments

import functions.filter_schemas as filter_schemas
import api.pboxes.schemas as pboxes_schemas

from functions.helpers import get_filters_pboxes
from datetime import datetime

router = APIRouter(tags=["pboxes"])


@router.get("/payboxes", response_model=pboxes_schemas.GetPayments)
async def read_payboxes_meta(token: str, limit: int = 100, offset: int = 0, sort: str = "created_at:desc",
                             filters: filter_schemas.PayboxesFiltersQuery = Depends()):
    """Получение счетов"""
    query = users_cboxes_relation.select(
        users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            filters = get_filters_pboxes(pboxes, filters)

            sort_list = sort.split(":")
            if sort_list[0] not in ["created_at", "updated_at"]:
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!")
            if sort_list[1] == "desc":
                q = pboxes.select(pboxes.c.cashbox == user.cashbox_id).filter(*filters).order_by(
                    desc(getattr(pboxes.c, sort_list[0]))).offset(offset).limit(limit)

            elif sort_list[1] == "asc":
                q = pboxes.select(pboxes.c.cashbox == user.cashbox_id).filter(*filters).order_by(
                    asc(getattr(pboxes.c, sort_list[0]))).offset(offset).limit(limit)
            else:
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!")

            pbox = await database.fetch_all(q)

            q = select(func.count(pboxes.c.id)).where(
                pboxes.c.cashbox == user.cashbox_id).filter(*filters)
            count = await database.fetch_one(q)

            return {"result": pbox, "count": count.count_1}

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.post("/payboxes", response_model=pboxes_schemas.Payboxes)
async def create_paybox(token: str, paybox_data: pboxes_schemas.PayboxesCreate):
    """Создание счета"""
    query = users_cboxes_relation.select(
        users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            created_date = datetime.utcnow().date()
            created_date_ts = int(datetime.timestamp(
                datetime.combine(created_date, datetime.min.time())))

            pbox_data_dict = paybox_data.dict()
            pbox_data_dict['balance'] = pbox_data_dict['start_balance']
            pbox_data_dict['balance_date'] = created_date_ts
            pbox_data_dict['cashbox'] = user.cashbox_id
            pbox_data_dict['update_start_balance'] = int(
                datetime.utcnow().timestamp())
            pbox_data_dict['update_start_balance_date'] = int(
                datetime.utcnow().timestamp())
            pbox_data_dict['created_at'] = int(datetime.utcnow().timestamp())
            pbox_data_dict['updated_at'] = int(datetime.utcnow().timestamp())

            q = pboxes.insert().values(pbox_data_dict)
            new_pbox_id = await database.execute(q)

            q = pboxes.select(pboxes.c.id == new_pbox_id)
            pbox = await database.fetch_one(q)

            await manager.send_message(token, {"action": "create", "target": "payboxes", "result": dict(pbox)})

            return pbox

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.put("/payboxes", response_model=pboxes_schemas.Payboxes)
async def update_paybox_data(token: str, pbox_data: pboxes_schemas.PayboxesEdit):
    """Обновление счета"""
    query = users_cboxes_relation.select(
        users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            pbox_data_dict = pbox_data.dict()
            del pbox_data_dict['id']

            new_pbox = {}

            q = pboxes.select(pboxes.c.id == pbox_data.id)
            pbox = await database.fetch_one(q)

            query = payments.select().where(or_(payments.c.paybox == pbox_data.id, payments.c.paybox_to ==
                                                pbox_data.id), payments.c.is_deleted == False, payments.c.cashbox == user.cashbox_id)
            pbox_payments = await database.fetch_all(query)

            for i, j in pbox_data_dict.items():
                if j is not None:
                    if i == "start_balance":
                        all_payments_amount = 0
                        for payment in pbox_payments:
                            today_ts = int(
                                datetime.timestamp(datetime.today()))
                            date_ts = int(payment.date)
                            if pbox['balance_date'] <= date_ts <= today_ts:
                                if payment.status:
                                    if payment.type == PaymentType.incoming:
                                        all_payments_amount += float(
                                            payment.amount)
                                    elif payment.type == PaymentType.transfer:
                                        all_payments_amount += float(
                                            payment.amount)
                                    else:
                                        all_payments_amount -= float(
                                            payment.amount)

                        new_pbox['balance'] = round(all_payments_amount + j, 2)
                        new_pbox['update_start_balance'] = int(
                            datetime.utcnow().timestamp())

                    if i == "balance_date":
                        all_payments_amount = 0
                        for payment in pbox_payments:
                            today_ts = int(
                                datetime.timestamp(datetime.today()))
                            date_ts = int(payment.date)
                            if j <= date_ts <= today_ts:
                                if payment.status:
                                    if payment.type == PaymentType.incoming:
                                        all_payments_amount += float(
                                            payment.amount)
                                    elif payment.type == PaymentType.transfer:
                                        all_payments_amount += float(
                                            payment.amount)
                                    else:
                                        all_payments_amount -= float(
                                            payment.amount)
                                        if payment.paybox_to:
                                            pbox_to = await database.fetch_one(
                                                pboxes.select(
                                                    pboxes.c.id == payment.paybox_to)
                                            )
                                            await database.execute(
                                                pboxes.update(pboxes.c.id == pbox_to.id).values({
                                                    "balance": round(float(pbox_to['balance']) + payment.amount, 2),
                                                    "update_start_balance_date": int(datetime.utcnow().timestamp())
                                                })
                                            )

                            # print(f"j:{j} - date_ts:{date_ts} - today_ts:{today_ts}")
                        # print(all_payments_amount)
                        new_pbox['balance'] = round(
                            float(pbox['start_balance']) + all_payments_amount, 2)
                        new_pbox['update_start_balance_date'] = int(
                            datetime.utcnow().timestamp())

                    new_pbox[i] = j

            if new_pbox:
                new_pbox['updated_at'] = int(datetime.utcnow().timestamp())

                q = pboxes.update().where(pboxes.c.id == pbox_data.id, pboxes.c.cashbox == user.cashbox_id).values(
                    new_pbox)
                await database.execute(q)

                q = pboxes.select().where(pboxes.c.id == pbox_data.id,
                                          pboxes.c.cashbox == user.cashbox_id)
                pbox = await database.fetch_one(q)

                await manager.send_message(token, {"action": "edit", "target": "payboxes", "result": dict(pbox)})

                return pbox

            else:
                raise HTTPException(status_code=400, detail="Неверный запрос!")

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, desc, asc, func

from ws_manager import manager

from database.db import database, users, users_cboxes_relation, cboxes

import functions.filter_schemas as filter_schemas
import api.users.schemas as user_schemas

from functions.helpers import get_filters_users, raise_wrong_token

from api.cashboxes.schemas import CashboxUpdate

router = APIRouter(tags=["cboxes"])


@router.get("/cashbox_users/", response_model=user_schemas.CBUsersList)
async def read_cashbox_users(
    token: str,
    limit: int = 100,
    offset: int = 0,
    sort: str = "created_at:desc",
    filters: filter_schemas.UsersFiltersQuery = Depends(),
):
    """Получение юзеров кассы"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    
    if not user or not user.status:
        raise_wrong_token()
    
    filters = get_filters_users(users, filters)
    users_list = []

    sort_list = sort.split(":")
    if sort_list[0] not in ["created_at", "updated_at"]:
        raise HTTPException(
            status_code=400, detail="Вы ввели некорректный параметр сортировки!"
        )
    
    # ОПТИМИЗИРОВАННЫЙ ЗАПРОС С JOIN
    if sort_list[1] == "desc":
        q = (
            select([
                users.c.id,
                users.c.external_id,
                users.c.photo,
                users.c.first_name,
                users.c.last_name,
                users.c.username,
                users_cboxes_relation.c.status,
                users_cboxes_relation.c.is_owner,
                users.c.created_at,  # ← Исправлено
                users.c.updated_at,  # ← Исправлено
                users_cboxes_relation.c.tags,
                users_cboxes_relation.c.timezone,
                users_cboxes_relation.c.payment_past_edit_days,
                users_cboxes_relation.c.shift_work_enabled
            ])
            .select_from(users_cboxes_relation.join(users, users.c.id == users_cboxes_relation.c.user))
            .where(users_cboxes_relation.c.cashbox_id == user.cashbox_id)
            .filter(*filters)
            .order_by(desc(getattr(users_cboxes_relation.c, sort_list[0])))
            .offset(offset)
            .limit(limit)
        )
    elif sort_list[1] == "asc":
        q = (
            select([
                users.c.id,
                users.c.external_id,
                users.c.photo,
                users.c.first_name,
                users.c.last_name,
                users.c.username,
                users_cboxes_relation.c.status,
                users_cboxes_relation.c.is_owner,
                users.c.created_at,  # ← Исправлено
                users.c.updated_at,  # ← Исправлено
                users_cboxes_relation.c.tags,
                users_cboxes_relation.c.timezone,
                users_cboxes_relation.c.payment_past_edit_days,
                users_cboxes_relation.c.shift_work_enabled
            ])
            .select_from(users_cboxes_relation.join(users, users.c.id == users_cboxes_relation.c.user))
            .where(users_cboxes_relation.c.cashbox_id == user.cashbox_id)
            .filter(*filters)
            .order_by(asc(getattr(users_cboxes_relation.c, sort_list[0])))
            .offset(offset)
            .limit(limit)
        )
    else:
        raise HTTPException(
            status_code=400, detail="Вы ввели некорректный параметр сортировки!"
        )

    results = await database.fetch_all(q)

    count_query = (
        select([func.count()])
        .select_from(users_cboxes_relation)
        .where(users_cboxes_relation.c.cashbox_id == user.cashbox_id)
        .filter(*filters)
    )
    count = await database.fetch_val(count_query)

    for result in results:
        user_dict = {
            "id": result.id,
            "external_id": result.external_id,
            "photo": result.photo,
            "first_name": result.first_name,
            "last_name": result.last_name,
            "username": result.username,
            "status": result.status,
            "is_admin": result.is_owner,
            "created_at": result.created_at,
            "updated_at": result.updated_at,
            "tags": result.tags,
            "timezone": result.timezone,
            "payment_past_edit_days": result.payment_past_edit_days,
            "shift_work_enabled": result.shift_work_enabled
        }
    users_list.append(user_dict)

    return {"result": users_list, "count": count}


@router.put("/cashbox_users/", response_model=user_schemas.CBUsers)
async def edit_cashbox_user(token: str, user_id: int, data: Optional[CashboxUpdate] = None, status: Optional[bool] = None):
    """Обновление статуса юзера кассы"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if not data:
        data = {}
    else:
        data = data.dict(exclude_unset=True)
    if status is not None:
        data["status"] = status

    if user:
        if user.is_owner:
            if data:
                q = (
                    users_cboxes_relation.update()
                    .where(
                        users_cboxes_relation.c.cashbox_id == user.cashbox_id,
                        users_cboxes_relation.c.user == user_id,
                    )
                    .values(**data)
                )
                await database.execute(q)

            q = users_cboxes_relation.select().where(
                users_cboxes_relation.c.cashbox_id == user.cashbox_id,
                users_cboxes_relation.c.user == user_id,
            )
            owner = await database.fetch_one(q)

            q = users.select(users.c.id == user_id)
            tg_acc = await database.fetch_one(q)

            user_dict = {
                "id": tg_acc.id,
                "photo": tg_acc.photo,
                "first_name": tg_acc.first_name,
                "last_name": tg_acc.last_name,
                "username": tg_acc.username,
                "status": owner.status,
                "is_admin": owner.is_owner,
                "created_at": tg_acc.created_at,
                "updated_at": tg_acc.updated_at,
                "tags": owner.tags,
                "timezone": owner.timezone,
                "payment_past_edit_days": owner.payment_past_edit_days,
                "shift_work_enabled": owner.shift_work_enabled  # ← Добавлено это поле
            }

            if data.get("status") is not None:
                await manager.send_message(
                    token, {"action": "edit", "target": "users", "result": user_dict}
                )

            return user_dict

    raise_wrong_token()


@router.get("/cashboxes_meta/")
async def read_payments_meta(token: str, limit: int = 100, offset: int = 0):
    """Мета юзеров кассы"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            cboxes_list = []
            q = (
                users_cboxes_relation.select()
                .where(
                    users_cboxes_relation.c.user == user.user,
                    users_cboxes_relation.c.status == True,
                )
                .offset(offset)
                .limit(limit)
            )
            cboxes_db = await database.fetch_all(q)

            for cbox in cboxes_db:
                q = cboxes.select(cboxes.c.id == cbox.cashbox_id)
                cbox_db = await database.fetch_one(q)

                cboxes_list.append(
                    {
                        "name": cbox_db.name,
                        "token": cbox.token,
                        "balance": cbox_db.balance,
                    }
                )

            resp = {"invite_token": None, "cboxes": cboxes_list}

            if user.is_owner:
                q = cboxes.select(cboxes.c.id == user.cashbox_id)
                cbox = await database.fetch_one(q)
                resp = {"invite_token": cbox.invite_token, "cboxes": cboxes_list}

            return resp

    raise_wrong_token()

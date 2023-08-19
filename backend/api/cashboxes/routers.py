from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, asc

from ws_manager import manager

from database.db import database, users, users_cboxes_relation, cboxes

import functions.filter_schemas as filter_schemas
import api.users.schemas as user_schemas

from functions.helpers import get_filters_users, raise_wrong_token

router = APIRouter(tags=["cboxes"])


@router.get("/cashbox_users", response_model=user_schemas.CBUsersList)
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
    if user:
        if user.status:
            filters = get_filters_users(users, filters)
            users_list = []
            count = 0

            sort_list = sort.split(":")
            if sort_list[0] not in ["created_at", "updated_at"]:
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!"
                )
            if sort_list[1] == "desc":
                q = (
                    users_cboxes_relation.select()
                    .where(users_cboxes_relation.c.cashbox_id == user.cashbox_id)
                    .filter(*filters)
                    .order_by(desc(getattr(users_cboxes_relation.c, sort_list[0])))
                    .offset(offset)
                    .limit(limit)
                )

            elif sort_list[1] == "asc":
                q = (
                    users_cboxes_relation.select()
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

            cb_users = await database.fetch_all(q)

            for u in cb_users:
                q = users.select(users.c.id == u.user).filter(*filters)
                tg_acc = await database.fetch_one(q)

                count += 1
                user_dict = {
                    "id": tg_acc.id,
                    "external_id": tg_acc.external_id,
                    "photo": tg_acc.photo,
                    "first_name": tg_acc.first_name,
                    "last_name": tg_acc.last_name,
                    "username": tg_acc.username,
                    "status": u.status,
                    "is_admin": u.is_owner,
                    "created_at": tg_acc.created_at,
                    "updated_at": tg_acc.updated_at,
                }

                users_list.append(user_dict)

            return {"result": users_list, "count": count}

    raise_wrong_token()


@router.put("/cashbox_users", response_model=user_schemas.CBUsers)
async def edit_cashbox_user_status(token: str, user_id: int, status: bool):
    """Обновление статуса юзера кассы"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.is_owner:
            q = (
                users_cboxes_relation.update()
                .where(
                    users_cboxes_relation.c.cashbox_id == user.cashbox_id,
                    users_cboxes_relation.c.user == user_id,
                )
                .values(status=status)
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
                "status": status,
                "is_admin": owner.is_owner,
            }

            await manager.send_message(
                token, {"action": "edit", "target": "users", "result": user_dict}
            )

            return user_dict

    raise_wrong_token()


@router.get("/cashboxes_meta")
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

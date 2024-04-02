from api.users import schemas as schemas
from fastapi import APIRouter
from functions import users as func
from database.db import database, users, users_cboxes_relation
from sqlalchemy import select, func as fsql

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/", response_model=schemas.CBUsers)
async def get_user_by_token_route(token: str):
    return await func.get_user_by_token(token=token)


@router.get("/list/", response_model=schemas.CBUsersListShort)
async def get_user_list(token: str):
    cashbox_query = select(users_cboxes_relation.c.cashbox_id).\
        where(users_cboxes_relation.c.token == token).subquery('cashbox_query')

    users_cashbox = select(
        users.c.id,
        users.c.external_id,
        users.c.first_name,
        users.c.last_name,
        users_cboxes_relation.c.status
    ).\
        where(users_cboxes_relation.c.cashbox_id == cashbox_query.c.cashbox_id).\
        join(users, users.c.id == users_cboxes_relation.c.user)

    users_list = await database.fetch_all(users_cashbox)
    count = await database.fetch_val(select(fsql.count(users_cashbox.c.id)))
    return {'result': users_list, 'count': count}

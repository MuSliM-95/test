from fastapi import APIRouter, Request, Depends, HTTPException
from sqlalchemy import select, func, desc, case, and_, text, asc
from database.db import database, amo_bots, table_triggers, table_triggers_events
from . import schemas
from functions.helpers import get_user_by_token


router = APIRouter(tags = ["amo_triggers"], prefix = "/settings")


async def verify_user(req: Request):
    user = await get_user_by_token(req.query_params["token"])
    if user:
        return user


@router.get("/triggers")
async def get_triggers_list(token: str, limit: int = 5, offset: int = 0, user = Depends(verify_user)):
    try:
        query = select(table_triggers).where(table_triggers.c.cashbox_id == user.get('cashbox_id'))
        triggers = await database.fetch_all(query.limit(limit).offset(offset))

        total = await database.fetch_val(select(func.count()).select_from(query))
        return {"items": triggers, "pageSize": limit, "total": total}
    except Exception as e:

        raise HTTPException(
            status_code = 432,
            detail = {"error_code": 500, "error": str(e)}
        )

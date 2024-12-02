from fastapi import APIRouter, Request, Depends, HTTPException
from sqlalchemy import select, func, desc, case, and_, text, asc
from database.db import database, amo_bots, table_triggers, table_triggers_events
from . import schemas
from functions.helpers import get_user_by_token
import uuid
from database.enums import TriggerType, TriggerTime


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


@router.get("/amo_bots")
async def get_amo_bots(token: str, filter: schemas.Filtersamobot = Depends(), user = Depends(verify_user)):
    filters = []
    try:
        if filter.name:
            filters.append(amo_bots.c.name.ilike(f"%{filter.name}%"))
        query = select(amo_bots).where(amo_bots.c.cashbox_id == user.get('cashbox_id')).filter(*filters)
        bots = await database.fetch_all(query)
        return bots
    except Exception as e:
        raise HTTPException(
            status_code = 432,
            detail = {"error_code": 500, "error": str(e)}
        )


@router.post("/triggers")
async def post_trigger(token: str, trigger: schemas.CreateTrigger, user = Depends(verify_user)):
    try:
        trigger_create = {
            **trigger, "key": str(uuid.uuid4().hex), "time": trigger.time*60 if trigger.time_variant == TriggerTime.minute
            else trigger.time*3600 if trigger.time_variant == TriggerTime.hour
            else trigger.time*86400 if trigger.time_variant == TriggerTime.day
            else 0}
        print(trigger)
        query = table_triggers.insert()
    except Exception as e:
        raise HTTPException(
            status_code = 432,
            detail = {"error_code": 500, "error": str(e)}
        )

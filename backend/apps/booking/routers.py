from fastapi import APIRouter, HTTPException, Depends, Request
from database.db import database, booking
from sqlalchemy import or_, and_, select
from functions.helpers import get_user_by_token


router = APIRouter(tags=["booking"])


@router.get("/booking/list")
async def get_list_booking(token: str):
    user = await get_user_by_token(token)
    result = await database.fetch_all(booking.select().where(booking.c.cashbox == user.cashbox_id))
    return result


@router.get("/booking/{idx}")
async def get_booking_by_idx(token: str, idx: int):
    user = await get_user_by_token( token)
    result = await database.fetch_one(booking.select().where(and_(booking.c.cashbox == user.cashbox_id, booking.c.id == idx)))
    return result

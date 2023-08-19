from typing import Optional

from fastapi import APIRouter, HTTPException
from sqlalchemy import desc

from database.db import database, warehouse_balances, warehouses

from . import schemas

from functions.helpers import datetime_to_timestamp, check_entity_exists
from functions.helpers import get_user_by_token

router = APIRouter(tags=["warehouse_balances"])


@router.get("/warehouse_balances/{warehouse_id}", response_model=int)
async def get_warehouse_current_balance(token: str, warehouse_id: int, nomenclature_id: int):
    """Получение текущего остатка товара по складу"""
    await get_user_by_token(token)
    await check_entity_exists(warehouses, warehouse_id)
    query = (
        warehouse_balances.select()
        .where(
            warehouse_balances.c.warehouse_id == warehouse_id,
            warehouse_balances.c.nomenclature_id == nomenclature_id,
        )
        .order_by(desc(warehouse_balances.c.created_at))
    )
    warehouse_db = await database.fetch_one(query)
    if not warehouse_db:
        return 0
    return warehouse_db.current_amount


@router.get("/warehouse_balances/", response_model=schemas.ListView)
async def get_warehouse_balances(
    token: str,
    warehouse_id: int,
    nomenclature_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Получение списка остатков склада"""
    await get_user_by_token(token)
    query = (
        warehouse_balances.select()
        .where(warehouse_balances.c.warehouse_id == warehouse_id)
        .limit(limit)
        .offset(offset)
    )
    if nomenclature_id is not None:
        query = query.where(warehouse_balances.c.nomenclature_id == nomenclature_id)
    warehouse_balances_db = await database.fetch_all(query)
    warehouse_balances_db = [*map(datetime_to_timestamp, warehouse_balances_db)]
    return warehouse_balances_db

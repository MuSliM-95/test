from typing import Optional
from fastapi_pagination import paginate, add_pagination
from api.pagination.pagination import Page
from fastapi import APIRouter
from sqlalchemy import select, func, desc, case

from database.db import database, warehouse_balances, warehouses, warehouse_register_movement, nomenclature, OperationType
from . import schemas

from functions.helpers import datetime_to_timestamp, check_entity_exists
from functions.helpers import get_user_by_token

router = APIRouter(tags=["warehouse_balances"])


@router.get("/warehouse_balances/{warehouse_id}/", response_model=int)
async def get_warehouse_current_balance(token: str, warehouse_id: int, nomenclature_id: int, organization_id: int):
    """Получение текущего остатка товара по складу"""
    await get_user_by_token(token)
    await check_entity_exists(warehouses, warehouse_id)
    query = (
        warehouse_balances.select()
        .where(
            warehouse_balances.c.warehouse_id == warehouse_id,
            warehouse_balances.c.nomenclature_id == nomenclature_id,
            warehouse_balances.c.organization_id == organization_id,
        )
        .order_by(desc(warehouse_balances.c.created_at))
    )
    warehouse_db = await database.fetch_one(query)
    if not warehouse_db:
        return 0
    return warehouse_db.current_amount


@router.get("/warehouse_balances/", response_model=Page[schemas.View])
async def get_warehouse_balances(
    token: str,
    warehouse_id: int,
    nomenclature_id: Optional[int] = None,
    organization_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Получение списка остатков склада"""
    await get_user_by_token(token)
    query = (
        select()
        .where(warehouse_balances.c.warehouse_id == warehouse_id)
        .limit(limit)
        .offset(offset)
    )
    if nomenclature_id is not None:
        query = query.where(warehouse_balances.c.nomenclature_id == nomenclature_id)
    if organization_id is not None:
        query = query.where(warehouse_balances.c.organization_id == organization_id)
    warehouse_balances_db = await database.fetch_all(query)
    warehouse_balances_db = [*map(datetime_to_timestamp, warehouse_balances_db)]
    return paginate(warehouse_balances_db)


@router.get("/alt_warehouse_balances/", response_model=Page[schemas.ViewAlt])
async def alt_get_warehouse_balances(
    token: str,
    warehouse_id: int,
    nomenclature_id: Optional[int] = None,
    organization_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Получение списка остатков склада"""

    selection_conditions = [warehouse_register_movement.c.warehouse_id == warehouse_id]
    if nomenclature_id is not None:
        selection_conditions.append(warehouse_register_movement.c.nomenclature_id == nomenclature_id)
    if organization_id is not None:
        selection_conditions.append(warehouse_register_movement.c.organization_id == organization_id)
    await get_user_by_token(token)
    q = case(
        [
            (
                warehouse_register_movement.c.type_amount == 'minus',
                warehouse_register_movement.c.amount * (-1)
            )
        ],
        else_=warehouse_register_movement.c.amount

    )
    query = (
        select(
            nomenclature.c.id,
            nomenclature.c.name,
            warehouse_register_movement.c.organization_id,
            warehouse_register_movement.c.warehouse_id,
            func.sum(q).label("current_amount"))
        .where(*selection_conditions)
        .limit(limit)
        .offset(offset)
    ).group_by(
               nomenclature.c.name,
               nomenclature.c.id,
               warehouse_register_movement.c.organization_id,
               warehouse_register_movement.c.warehouse_id
               )\
        .select_from(warehouse_register_movement
                     .join(nomenclature,
                           warehouse_register_movement.c.nomenclature_id == nomenclature.c.id
                           ))

    warehouse_balances_db = await database.fetch_all(query)
    # warehouse_balances_db = [*map(datetime_to_timestamp, warehouse_balances_db)]
    return paginate(warehouse_balances_db)

add_pagination(router)
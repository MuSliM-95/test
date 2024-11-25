from typing import Optional, List
from fastapi_pagination import paginate, add_pagination
from api.pagination.pagination import Page
from fastapi import APIRouter, status, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import select, func, desc, case, delete
from fastapi.encoders import jsonable_encoder
from database.db import database, warehouse_balances, warehouses, warehouse_register_movement, nomenclature, OperationType, organizations, categories
from . import schemas
from datetime import datetime

from functions.helpers import datetime_to_timestamp, check_entity_exists
from functions.helpers import get_user_by_token

router = APIRouter(tags=["warehouse_balances"])


@database.transaction()
@router.get("/warehouse_balances/clearQuantity/category/{id_category}", status_code = status.HTTP_202_ACCEPTED)
async def clear_quantity(token: str, id_category: int, warehouse_id: int, date_from: Optional[int] = None, date_to: Optional[int] = None):
    try:
        await get_user_by_token(token)

        dates_arr = []

        if date_to and not date_from:
            dates_arr.append(warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to))
        if date_to and date_from:
            dates_arr.append(warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to))
            dates_arr.append(warehouse_register_movement.c.created_at >= datetime.fromtimestamp(date_from))
        if not date_to and date_from:
            dates_arr.append(warehouse_register_movement.c.created_at >= datetime.fromtimestamp(date_from))

        selection_conditions = [
            warehouse_register_movement.c.warehouse_id == warehouse_id,
            nomenclature.c.category == id_category,
            *dates_arr
        ]

        query = (
            select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.category,
                warehouse_register_movement.c.id.label("id_movement"),
                warehouse_register_movement.c.type_amount,
                warehouse_register_movement.c.amount,
                warehouse_register_movement.c.organization_id,
                warehouse_register_movement.c.warehouse_id
            )
            .where(*selection_conditions)
        )\
            .select_from(warehouse_register_movement
                          .join(nomenclature,
                                 warehouse_register_movement.c.nomenclature_id == nomenclature.c.id
                                 ))

        warehouse_balances_db = await database.fetch_all(query)
        query_delete_warehouse_register_movement_by_category = \
            warehouse_register_movement.delete().\
            where(
                warehouse_register_movement.c.id.in_(
                    [item.id_movement for item in warehouse_balances_db]
                )
            )
        await database.execute(query_delete_warehouse_register_movement_by_category)
        return warehouse_balances_db
    except HTTPException as e:
        raise HTTPException(status_code = 432, detail = str(e.detail))


@database.transaction()
@router.get("/warehouse_balances/clearQuantity/product/{id_product}", status_code = status.HTTP_202_ACCEPTED)
async def clear_quantity(token: str, id_product: int, warehouse_id: int, date_from: Optional[int] = None, date_to: Optional[int] = None):
    try:
        await get_user_by_token(token)

        dates_arr = []

        if date_to and not date_from:
            dates_arr.append(warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to))
        if date_to and date_from:
            dates_arr.append(warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to))
            dates_arr.append(warehouse_register_movement.c.created_at >= datetime.fromtimestamp(date_from))
        if not date_to and date_from:
            dates_arr.append(warehouse_register_movement.c.created_at >= datetime.fromtimestamp(date_from))

        selection_conditions = [
            warehouse_register_movement.c.warehouse_id == warehouse_id,
            nomenclature.c.id == id_product,
            *dates_arr
        ]

        query = (
            select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.category,
                warehouse_register_movement.c.id.label("id_movement"),
                warehouse_register_movement.c.type_amount,
                warehouse_register_movement.c.amount,
                warehouse_register_movement.c.organization_id,
                warehouse_register_movement.c.warehouse_id
            )
            .where(*selection_conditions)
        )\
            .select_from(warehouse_register_movement
                          .join(nomenclature,
                                 warehouse_register_movement.c.nomenclature_id == nomenclature.c.id
                                 ))

        warehouse_balances_db = await database.fetch_one(query)

        if warehouse_balances_db:
            query_delete_warehouse_register_movement_by_category =\
                warehouse_register_movement.delete().\
                    where(
                    warehouse_register_movement.c.id == warehouse_balances_db.get("id_movement")
                )
            await database.execute(query_delete_warehouse_register_movement_by_category)
            return warehouse_balances_db
    except HTTPException as e:
        raise HTTPException(status_code = 432, detail = str(e.detail))


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


@router.get("/alt_warehouse_balances/", response_model=schemas.ViewRes)
async def alt_get_warehouse_balances(
    token: str,
    warehouse_id: int,
    nomenclature_id: Optional[int] = None,
    organization_id: Optional[int] = None,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Получение списка остатков склада"""

    dates_arr = []

    if date_to and not date_from:
        dates_arr.append(warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to))
    if date_to and date_from:
        dates_arr.append(warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to))
        dates_arr.append(warehouse_register_movement.c.created_at >= datetime.fromtimestamp(date_from))
    if not date_to and date_from:
        dates_arr.append(warehouse_register_movement.c.created_at >= datetime.fromtimestamp(date_from))


    selection_conditions = [warehouse_register_movement.c.warehouse_id == warehouse_id, *dates_arr]
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
            nomenclature.c.category,
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
            nomenclature.c.category,
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

    warehouse_balances_db_curr = await database.fetch_all(query)


    # warehouse_balances_db = [*map(datetime_to_timestamp, warehouse_balances_db)]
    res = []

    categories_db = await database.fetch_all(categories.select())

    res_with_cats = []

    for warehouse_balance in warehouse_balances_db:

        current = [item for item in warehouse_balances_db_curr if item.id == warehouse_balance.id]

        balance_dict = dict(warehouse_balance)

        organization_db = await database.fetch_one(organizations.select().where(organizations.c.id == warehouse_balance.organization_id))
        warehouse_db = await database.fetch_one(warehouses.select().where(warehouses.c.id == warehouse_balance.warehouse_id))

        plus_amount = 0
        minus_amount = 0

        register_q = warehouse_register_movement.select().where(
            warehouse_register_movement.c.warehouse_id == warehouse_id, 
            warehouse_register_movement.c.nomenclature_id == warehouse_balance.id,
            *dates_arr
        )\
        .order_by(warehouse_register_movement.c.id)


        register_events = await database.fetch_all(register_q)

        for reg_event in register_events:
            if reg_event.type_amount == "plus":
                plus_amount += reg_event.amount
            else:
                minus_amount += reg_event.amount

        print(current)
        balance_dict['now_ost'] = current[0].current_amount
        balance_dict['start_ost'] = balance_dict['current_amount'] - plus_amount + minus_amount
        balance_dict['plus_amount'] = plus_amount
        balance_dict['minus_amount'] = minus_amount
        balance_dict['organization_name'] = organization_db.short_name
        balance_dict['warehouse_name'] = warehouse_db.name

        res.append(balance_dict)

    for category in categories_db:
        cat_childrens = [item for item in res if item['category'] == category.id]

        if len(cat_childrens) > 0:
            res_with_cats.append(
                {
                    "name": category.name,
                    "key": category.id,
                    "children": cat_childrens
                }
            )
        
    none_childrens = [item for item in res if item['category'] == None]
    res_with_cats.append(
                {
                    "name": "Без категории",
                    "key": 0,
                    "children": none_childrens
                }
            )


    return {"result": res_with_cats}

add_pagination(router)
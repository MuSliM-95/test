from typing import Optional

import api.warehouses.schemas as schemas
from common.geocoders.instance import geocoder
from database.db import database, warehouse_hash, warehouses
from fastapi import APIRouter, HTTPException
from functions.helpers import (
    check_entity_exists,
    create_entity_hash,
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
    update_entity_hash,
)
from sqlalchemy import func, select
from ws_manager import manager

router = APIRouter(tags=["warehouses"])


@router.get("/warehouses/{idx}/", response_model=schemas.Warehouse)
async def get_warehouse_by_id(token: str, idx: int):
    user = await get_user_by_token(token)
    warehouse_db = await get_entity_by_id(warehouses, idx, user.cashbox_id)
    return datetime_to_timestamp(warehouse_db)


@router.get("/warehouses/", response_model=schemas.WarehouseListGet)
async def get_warehouses(
    token: str,
    name: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
):
    user = await get_user_by_token(token)

    filters = [
        warehouses.c.cashbox == user.cashbox_id,
        warehouses.c.is_deleted.is_not(True),
    ]

    if name:
        filters.append(warehouses.c.name.ilike(f"%{name}%"))

    query = warehouses.select().where(*filters).limit(limit).offset(offset)
    items = await database.fetch_all(query)
    items = [datetime_to_timestamp(x) for x in items]

    count_query = select(func.count(warehouses.c.id)).where(*filters)
    count = await database.fetch_one(count_query)

    return {
        "result": items,
        "count": count.count_1,
    }


@router.post("/warehouses/", response_model=schemas.Warehouse)
async def new_warehouse(
    token: str,
    warehouse: schemas.WarehouseCreate,
):
    user = await get_user_by_token(token)

    values = warehouse.dict()
    values["owner"] = user.id
    values["cashbox"] = user.cashbox_id

    if values.get("parent") is not None:
        await check_entity_exists(
            warehouses,
            values["parent"],
            user.cashbox_id,
        )

    if values.get("address"):
        geo = await geocoder.validate_address(values["address"])
        if geo:
            values.update(
                {
                    "address": ", ".join(
                        filter(
                            None,
                            [
                                geo.country,
                                geo.state,
                                geo.city,
                                geo.street,
                                geo.housenumber,
                                geo.postcode,
                            ],
                        )
                    ),
                    "latitude": geo.latitude,
                    "longitude": geo.longitude,
                }
            )
        warehouse_id = await database.execute(warehouses.insert().values(values))

    await create_entity_hash(
        table=warehouses,
        table_hash=warehouse_hash,
        idx=warehouse_id,
    )

    warehouse_db = await get_entity_by_id(
        warehouses,
        warehouse_id,
        user.cashbox_id,
    )

    warehouse_db = datetime_to_timestamp(warehouse_db)

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "warehouses",
            "result": warehouse_db,
        },
    )

    return warehouse_db


@router.patch("/warehouses/{idx}/", response_model=schemas.Warehouse)
async def edit_warehouse(
    token: str,
    idx: int,
    warehouse: schemas.WarehouseUpdate,
):
    user = await get_user_by_token(token)

    await get_entity_by_id(warehouses, idx, user.cashbox_id)

    values = warehouse.dict(exclude_unset=True)
    if not values:
        raise HTTPException(400, "no fields to update")

    if values.get("parent") is not None:
        await check_entity_exists(
            warehouses,
            values["parent"],
            user.cashbox_id,
        )

    await database.execute(
        warehouses.update()
        .where(
            warehouses.c.id == idx,
            warehouses.c.cashbox == user.cashbox_id,
        )
        .values(**values)
    )

    warehouse_db = await get_entity_by_id(
        warehouses,
        idx,
        user.cashbox_id,
    )

    await update_entity_hash(
        table=warehouses,
        table_hash=warehouse_hash,
        entity=warehouse_db,
    )

    warehouse_db = datetime_to_timestamp(warehouse_db)

    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "warehouses",
            "result": warehouse_db,
        },
    )

    return warehouse_db


@router.delete("/warehouses/{idx}/", response_model=schemas.Warehouse)
async def delete_warehouse(token: str, idx: int):
    user = await get_user_by_token(token)

    await get_entity_by_id(warehouses, idx, user.cashbox_id)

    await database.execute(
        warehouses.update()
        .where(
            warehouses.c.id == idx,
            warehouses.c.cashbox == user.cashbox_id,
        )
        .values(is_deleted=True)
    )

    warehouse_db = await get_entity_by_id(
        warehouses,
        idx,
        user.cashbox_id,
    )

    warehouse_db = datetime_to_timestamp(warehouse_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "warehouses",
            "result": warehouse_db,
        },
    )

    return warehouse_db

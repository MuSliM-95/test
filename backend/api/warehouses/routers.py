from typing import Optional

import api.warehouses.schemas as schemas
from database.db import database, warehouses, warehouse_hash
from common.geocoders.instance import geocoder
from database.db import database, warehouses, warehouse_hash
from fastapi import APIRouter, HTTPException
from functions.helpers import check_entity_exists, datetime_to_timestamp, get_entity_by_id, get_user_by_token, create_entity_hash, update_entity_hash
from sqlalchemy import func, select
from ws_manager import manager

router = APIRouter(tags=["warehouses"])


@router.get("/warehouses/{idx}/", response_model=schemas.Warehouse)
async def get_warehouse_by_id(token: str, idx: int):
    """Получение склада по ID"""
    user = await get_user_by_token(token)
    warehouse_db = await get_entity_by_id(warehouses, idx, user.cashbox_id)
    warehouse_db = datetime_to_timestamp(warehouse_db)
    return warehouse_db


@router.get("/warehouses/", response_model=schemas.WarehouseListGet)
async def get_warehouses(token: str, name: Optional[str] = None, limit: int = 100, offset: int = 0):
    """Получение списка складов"""
    user = await get_user_by_token(token)
    filters = [
        warehouses.c.cashbox == user.cashbox_id,
        warehouses.c.is_deleted.is_not(True),
    ]

    if name:
        filters.append(
            warehouses.c.name.ilike(f"%{name}%"),
        )

    query = warehouses.select().where(*filters).limit(limit).offset(offset)

    warehouses_db = await database.fetch_all(query)
    warehouses_db = [*map(datetime_to_timestamp, warehouses_db)]

    query = select(func.count(warehouses.c.id)).where(*filters)

    warehouses_db_count = await database.fetch_one(query)

    return {"result": warehouses_db, "count": warehouses_db_count.count_1}


@router.post("/warehouses/", response_model=schemas.WarehouseList)
async def new_warehouse(token: str, warehouses_data: schemas.WarehouseCreateMass):
    """Создание склада"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    warehouses_cache = set()
    exceptions = []
    for warehouse_values in warehouses_data.dict()["__root__"]:
        warehouse_values["owner"] = user.id
        warehouse_values["cashbox"] = user.cashbox_id

        if warehouse_values.get("parent") is not None:
            if warehouse_values["parent"] not in warehouses_cache:
                try:
                    await check_entity_exists(warehouses, warehouse_values["parent"], user.id)
                    warehouses_cache.add(warehouse_values["parent"])
                except HTTPException as e:
                    exceptions.append(str(warehouse_values) + " " + e.detail)
                    continue

        if warehouse_values.get("address") is not None:

            structured_geo = await geocoder.validate_address(warehouse_values.get("address"))

            if structured_geo is None:
                exceptions.append(str(warehouse_values) + " incorrect address field")
                continue

            warehouse_values.update(
                {
                    "address": ", ".join(
                        filter(None, [
                            structured_geo.country,
                            structured_geo.state,
                            structured_geo.city,
                            structured_geo.street,
                            structured_geo.housenumber,
                            structured_geo.postcode,
                        ])
                    ),
                    "latitude": structured_geo.latitude,
                    "longitude": structured_geo.longitude,
                }
            )

        query = warehouses.insert().values(warehouse_values)
        warehouse_id = await database.execute(query)
        await create_entity_hash(table=warehouses, table_hash=warehouse_hash, idx=warehouse_id)
        inserted_ids.add(warehouse_id)

    query = warehouses.select().where(warehouses.c.cashbox == user.cashbox_id, warehouses.c.id.in_(inserted_ids))
    warehouses_db = await database.fetch_all(query)
    warehouses_db = [*map(datetime_to_timestamp, warehouses_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "warehouses",
            "result": warehouses_db,
        },
    )

    if exceptions:
        raise HTTPException(400, "Не были добавлены следующие записи: " + ", ".join(exceptions))

    return warehouses_db


@router.patch("/warehouses/{idx}/", response_model=schemas.Warehouse)
async def edit_warehouse(
    token: str,
    idx: int,
    warehouse: schemas.WarehouseEdit,
):
    """Редактирование склада"""
    user = await get_user_by_token(token)
    warehouse_db = await get_entity_by_id(warehouses, idx, user.cashbox_id)
    warehouse_values = warehouse.dict(exclude_unset=True)

    if warehouse_values:
        if warehouse_values.get("parent") is not None:
            await check_entity_exists(warehouses, warehouse_values["parent"], user.id)

        query = (
            warehouses.update().where(warehouses.c.id == idx, warehouses.c.cashbox == user.cashbox_id).values(warehouse_values)
        )
        await database.execute(query)
        warehouse_db = await get_entity_by_id(warehouses, idx,user.cashbox_id)
        await update_entity_hash(table=warehouses, table_hash=warehouse_hash, entity=warehouse_db)

    warehouse_db = datetime_to_timestamp(warehouse_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "warehouses", "result": warehouse_db},
    )

    return warehouse_db


@router.delete("/warehouses/{idx}/", response_model=schemas.Warehouse)
async def delete_warehouse(token: str, idx: int):
    """Удаление склада"""
    user = await get_user_by_token(token)

    await get_entity_by_id(warehouses, idx, user.id)

    query = (
        warehouses.update().where(warehouses.c.id == idx, warehouses.c.cashbox == user.cashbox_id).values({"is_deleted": True})
    )
    await database.execute(query)

    query = warehouses.select().where(warehouses.c.id == idx, warehouses.c.cashbox == user.cashbox_id)
    warehouse_db = await database.fetch_one(query)
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

import time
from typing import List, Optional

from starlette import status

import api.nomenclature.schemas as schemas
from api.nomenclature.web.pagination.NomenclatureFilter import NomenclatureFilter, SortOrder
from database.db import categories, database, manufacturers, nomenclature, nomenclature_barcodes, prices, price_types, \
    warehouse_register_movement, warehouses, units, warehouse_balances, nomenclature_groups_value
from fastapi import APIRouter, HTTPException, Depends
from fastapi.params import Body

from functions.helpers import (
    check_entity_exists,
    check_unit_exists,
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
    nomenclature_unit_id_to_name,
)
from sqlalchemy import func, select, and_, desc, asc, case, cast, ARRAY, null, or_
from sqlalchemy.sql.functions import coalesce
from ws_manager import manager
import memoization


router = APIRouter(tags=["nomenclature"])


@router.patch("/nomenclature/barcode")
async def patch_nomenclature_barcodes(token: str, barcodes: List[schemas.NomenclaturesListPatch]):
    """Изменение штрихкодов категории по ID"""
    user = await get_user_by_token(token)

    errors = []

    for barcode in barcodes:
        query = nomenclature.select().where(
            nomenclature.c.id == barcode.idx,
            nomenclature.c.cashbox == user.cashbox_id,
            nomenclature.c.is_deleted.is_not(True),
        )
        nomenclature_db = await database.fetch_one(query)
        if not nomenclature_db:
            errors.append({
                "idx": barcode.idx,
                "error_code": 404,
                "type_error": "Nomenclature not found"
            })
            continue

        query = (
            nomenclature_barcodes.select()
            .where(nomenclature_barcodes.c.nomenclature_id == barcode.idx)
        )
        barcode_ex_list = await database.fetch_all(query)
        barcodes = [barcode_info.code for barcode_info in barcode_ex_list]

        async with database.transaction():
            if barcode.new_barcode in barcodes:
                query = (
                    nomenclature_barcodes.delete().where(and_(
                        nomenclature_barcodes.c.nomenclature_id == barcode.idx,
                        nomenclature_barcodes.c.code == barcode.new_barcode
                    ))
                )
                await database.execute(query)

                query = (
                    nomenclature_barcodes.update().where(and_(
                        nomenclature_barcodes.c.nomenclature_id == barcode.idx,
                        nomenclature_barcodes.c.code == barcode.old_barcode
                    ))
                    .values({
                        "code": barcode.new_barcode
                    })
                )
                await database.execute(query)
            elif not barcodes:
                query = (
                    nomenclature_barcodes.insert()
                    .values({
                        "nomenclature_id": barcode.idx,
                        "code": barcode.new_barcode
                    })
                )
                await database.execute(query)
            else:
                query = (
                    nomenclature_barcodes.update().where(and_(
                        nomenclature_barcodes.c.nomenclature_id == barcode.idx,
                        nomenclature_barcodes.c.code == barcode.old_barcode
                    ))
                    .values({
                        "code": barcode.new_barcode
                    })
                )
                await database.execute(query)
    return {
        "errors": errors
    }


@router.get("/nomenclature/{idx}/barcode")
async def get_nomenclature_barcodes(token: str, idx: int):
    """Получение штрихкодов категории по ID"""
    user = await get_user_by_token(token)

    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)

    query = nomenclature_barcodes.select().where(nomenclature_barcodes.c.nomenclature_id == idx)
    barcodes_list = await database.fetch_all(query)

    return [barcode_info.code for barcode_info in barcodes_list]


@router.post("/nomenclature/{idx}/barcode")
async def add_barcode_to_nomenclature(token: str, idx: int, barcode: schemas.NomenclatureBarcodeCreate):
    """Добавление штрихкода к категории по ID"""
    user = await get_user_by_token(token)

    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
    query = nomenclature_barcodes.select().where(and_(
        nomenclature_barcodes.c.nomenclature_id == idx,
        nomenclature_barcodes.c.code == barcode.barcode
    ))
    barcode_ex = await database.fetch_one(query)

    if barcode_ex:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Данный штрихкод уже привязан к этому товару")

    query = nomenclature_barcodes.insert().values({
        "nomenclature_id": idx,
        "code": barcode.barcode
    })
    await database.execute(query)


@router.delete("/nomenclature/{idx}/barcode")
async def delete_barcode_to_nomenclature(token: str, idx: int, barcode: schemas.NomenclatureBarcodeCreate):
    """Добавление штрихкода к категории по ID"""
    user = await get_user_by_token(token)

    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
    query = nomenclature_barcodes.delete().where(and_(
        nomenclature_barcodes.c.nomenclature_id == idx,
        nomenclature_barcodes.c.code == barcode.barcode
    ))
    await database.execute(query)


@router.post("/nomenclatures/", response_model=schemas.NomenclatureListGetRes)
async def get_nomenclature_by_ids(token: str, ids: List[int] = Body(..., example=[1, 2, 3]), with_prices: bool = False, with_balance: bool = False):
    """Получение списка номенклатур по списку ID категорий"""
    user = await get_user_by_token(token)

    if not ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Список ID не должен быть пустым")

    query = (
        select(
            nomenclature,
            units.c.convent_national_view.label("unit_name"),
            func.array_remove(func.array_agg(func.distinct(nomenclature_barcodes.c.code)), None).label("barcodes")
        )
        .select_from(nomenclature)
        .join(units, units.c.id == nomenclature.c.unit, full=True)
        .join(nomenclature_barcodes, nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id, full=True)
        .where(
            nomenclature.c.cashbox == user.cashbox_id,
            nomenclature.c.is_deleted.is_not(True),
            nomenclature.c.category.in_(ids)
        )
        .group_by(nomenclature.c.id, units.c.convent_national_view)
        .order_by(asc(nomenclature.c.id))
    )

    nomenclature_db = await database.fetch_all(query)
    nomenclature_db = [*map(datetime_to_timestamp, nomenclature_db)]

    for nomenclature_info in nomenclature_db:
        if with_prices:
            price = await database.fetch_all(
                select(prices.c.price, price_types.c.name.label('price_type')).
                where(prices.c.nomenclature == nomenclature_info['id']).
                select_from(prices).
                join(price_types, price_types.c.id == prices.c.price_type)
            )
            nomenclature_info["prices"] = price

        if with_balance:
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
                    warehouses.c.name.label("warehouse_name"),
                    nomenclature.c.id,
                    warehouse_register_movement.c.nomenclature_id,
                    func.sum(q).label("current_amount"))
                .where(
                    nomenclature.c.id == nomenclature_info['id'],
                    warehouse_register_movement.c.cashbox_id == user.cashbox_id
                )
            ).group_by(
                warehouses.c.name,
                nomenclature.c.id,
                warehouse_register_movement.c.nomenclature_id,
            ) \
                .select_from(warehouse_register_movement
                             .join(warehouses, warehouse_register_movement.c.warehouse_id == warehouses.c.id))

            balances_list = await database.fetch_all(query)
            nomenclature_info["balances"] = balances_list

    query = select(func.count(nomenclature.c.id)).where(
        nomenclature.c.cashbox == user.cashbox_id,
        nomenclature.c.is_deleted.is_not(True),
        nomenclature.c.id.in_(ids)
    )
    nomenclature_db_count = await database.fetch_val(query)

    return {"result": nomenclature_db, "count": nomenclature_db_count}


@router.get("/nomenclature/", response_model=schemas.NomenclatureListGetRes)
async def get_nomenclature(
        token: str,
        name: Optional[str] = None,
        barcode: Optional[str] = None,
        category: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
        with_prices: bool = False,
        with_balance: bool = False,
        only_main_from_group: bool = False,
        filters_query: NomenclatureFilter = Depends(),
):
    start_time = time.time()
    """Получение списка категорий"""
    user = await get_user_by_token(token)

    if name and barcode:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Укажите только один из параметров: 'name' или 'barcode'")

    query = (
        select(
            nomenclature,
            nomenclature_groups_value.c.group_id,
            nomenclature_groups_value.c.is_main,
            units.c.convent_national_view.label("unit_name"),
            func.array_remove(func.array_agg(func.distinct(nomenclature_barcodes.c.code)), None).label("barcodes")
        )
        .select_from(nomenclature)
        .outerjoin(nomenclature_groups_value, nomenclature_groups_value.c.nomenclature_id == nomenclature.c.id)
        .join(units, units.c.id == nomenclature.c.unit, full=True)
        .join(nomenclature_barcodes, nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id, full=True)
    )

    filters = [
        nomenclature.c.cashbox == user.cashbox_id,
        nomenclature.c.is_deleted.is_not(True),
    ]

    if name:
        filters.append(nomenclature.c.name.ilike(f"%{name}%"))
    if barcode:
        filters.append(nomenclature_barcodes.c.code == barcode)
    if category:
        filters.append(nomenclature.c.category == category)
    if only_main_from_group:
        filters.append(or_(
            nomenclature_groups_value.c.is_main.is_(True),
            nomenclature_groups_value.c.is_main.is_(None),
        ))

    order_by_condition = asc(nomenclature.c.id)

    if filters_query.order_created_at:
        if filters_query.order_created_at == SortOrder.ASC:
            order_by_condition = asc(nomenclature.c.created_at)
        elif filters_query.order_created_at == SortOrder.DESC:
            order_by_condition = desc(nomenclature.c.created_at)
    elif filters_query.order_price:
        price_subquery = (
            select(
                prices.c.nomenclature.label("nomenclature_id"),
                func.min(prices.c.price).label("min_price")
            )
            .group_by(prices.c.nomenclature)
            .subquery()
        )
        query = query.outerjoin(price_subquery, price_subquery.c.nomenclature_id == nomenclature.c.id).group_by(price_subquery.c.min_price)

        if filters_query.order_price == SortOrder.ASC:
            order_by_condition = asc(price_subquery.c.min_price)
        else:
            order_by_condition = desc(price_subquery.c.min_price)
    elif filters_query.order_name:
        if filters_query.order_name == SortOrder.ASC:
            order_by_condition = asc(func.upper(func.left(nomenclature.c.name, 1)))
        elif filters_query.order_name == SortOrder.DESC:
            order_by_condition = desc(func.upper(func.left(nomenclature.c.name, 1)))

    query = query.where(*filters).limit(limit).offset(offset).group_by(
        nomenclature.c.id,
        units.c.convent_national_view,
        nomenclature_groups_value.c.group_id,
        nomenclature_groups_value.c.is_main
    ).order_by(order_by_condition)
    nomenclature_db = await database.fetch_all(query)
    nomenclature_db = [*map(datetime_to_timestamp, nomenclature_db)]

    print(f"Получение номенклатур: {time.time() - start_time}")

    for nomenclature_info in nomenclature_db:
        time_start_2 = time.time()
        if with_prices:
            price = await database.fetch_all(
                select(prices.c.price, price_types.c.name.label('price_type')).
                where(prices.c.nomenclature == nomenclature_info['id']).
                select_from(prices).
                join(price_types, price_types.c.id == prices.c.price_type)
            )
            nomenclature_info["prices"] = price
        if with_balance:
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
                    warehouses.c.name.label("warehouse_name"),
                    nomenclature.c.id,
                    warehouse_register_movement.c.nomenclature_id,
                    func.sum(q).label("current_amount"))
                .where(
                    nomenclature.c.id == nomenclature_info['id'],
                    warehouse_register_movement.c.cashbox_id == user.cashbox_id

                )
                .limit(limit)
                .offset(offset)
            ).group_by(
                warehouses.c.name,
                nomenclature.c.id,
                warehouse_register_movement.c.nomenclature_id,
            ) \
                .select_from(warehouse_register_movement
                             .join(warehouses, warehouse_register_movement.c.warehouse_id == warehouses.c.id))

            warehouse_balances_db = await database.fetch_all(query)

            nomenclature_info["balances"] = warehouse_balances_db
            print(nomenclature_info["balances"])
        print(f"Итерация цикла: {time.time() - time_start_2}")

    query = select(func.count(nomenclature.c.id)).where(*filters)
    nomenclature_db_count = await database.fetch_val(query)

    print(f"Окончание работы эндпоинта: {time.time() - start_time}")
    return {"result": nomenclature_db, "count": nomenclature_db_count}


@router.post("/nomenclature/", response_model=schemas.NomenclatureList)
async def new_nomenclature(token: str, nomenclature_data: schemas.NomenclatureCreateMass):
    """Создание категории"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    categories_cache = set()
    manufacturers_cache = set()
    units_cache = set()
    exceptions = []
    for nomenclature_values in nomenclature_data.dict()["__root__"]:
        nomenclature_values["cashbox"] = user.cashbox_id
        nomenclature_values["owner"] = user.id
        nomenclature_values["is_deleted"] = False

        if nomenclature_values.get("category") is not None:
            if nomenclature_values["category"] not in categories_cache:
                try:
                    await check_entity_exists(categories, nomenclature_values["category"], user.cashbox_id)
                    categories_cache.add(nomenclature_values["category"])
                except HTTPException as e:
                    exceptions.append(str(nomenclature_values) + " " + e.detail)
                    continue

        if nomenclature_values.get("manufacturer") is not None:
            if nomenclature_values["manufacturer"] not in manufacturers_cache:
                try:
                    await check_entity_exists(manufacturers, nomenclature_values["manufacturer"], user.id)
                    manufacturers_cache.add(nomenclature_values["manufacturer"])
                except HTTPException as e:
                    exceptions.append(str(nomenclature_values) + " " + e.detail)
                    continue

        if nomenclature_values.get("unit") is not None:
            if nomenclature_values["unit"] not in units_cache:
                try:
                    await check_unit_exists(nomenclature_values["unit"])
                    units_cache.add(nomenclature_values["unit"])
                except HTTPException as e:
                    exceptions.append(str(nomenclature_values) + " " + e.detail)
                    continue

        query = nomenclature.insert().values(nomenclature_values)
        nomenclature_id = await database.execute(query)
        inserted_ids.add(nomenclature_id)

    query = nomenclature.select().where(nomenclature.c.cashbox == user.cashbox_id, nomenclature.c.id.in_(inserted_ids))
    nomenclature_db = await database.fetch_all(query)
    nomenclature_db = [*map(datetime_to_timestamp, nomenclature_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "nomenclature",
            "result": nomenclature_db,
        },
    )

    if exceptions:
        raise HTTPException(400, "Не были добавлены следующие записи: " + ", ".join(exceptions))

    return nomenclature_db


@router.patch("/nomenclature/{idx}/", response_model=schemas.Nomenclature)
async def edit_nomenclature(
        token: str,
        idx: int,
        nomenclature_data: schemas.NomenclatureEdit,
):
    """Редактирование категории"""
    user = await get_user_by_token(token)
    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
    nomenclature_values = nomenclature_data.dict(exclude_unset=True)

    if nomenclature_values:
        if nomenclature_values.get("category") is not None:
            await check_entity_exists(categories, nomenclature_values["category"], user.id)
        if nomenclature_values.get("manufacturer") is not None:
            await check_entity_exists(manufacturers, nomenclature_values["manufacturer"], user.id)
        if nomenclature_values.get("unit") is not None:
            await check_unit_exists(nomenclature_values["unit"])

        query = (
            nomenclature.update()
            .where(nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id)
            .values(nomenclature_values)
        )
        await database.execute(query)
        nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)

    nomenclature_db = datetime_to_timestamp(nomenclature_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "nomenclature", "result": nomenclature_db},
    )

    return nomenclature_db


@router.patch("/nomenclature/", response_model=List[schemas.Nomenclature])
async def edit_nomenclature_mass(
        token: str,
        nomenclature_data: List[schemas.NomenclatureEditMass],
):
    """Редактирование номенклатуры пачкой"""
    user = await get_user_by_token(token)
    response_body = []
    for nomenclature_in_list in nomenclature_data:
        idx = nomenclature_in_list.id
        nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
        nomenclature_values = nomenclature_in_list.dict(exclude_unset=True)

        del nomenclature_values["id"]

        if nomenclature_values:
            if nomenclature_values.get("category") is not None:
                await check_entity_exists(categories, nomenclature_values["category"], user.id)
            if nomenclature_values.get("manufacturer") is not None:
                await check_entity_exists(manufacturers, nomenclature_values["manufacturer"], user.id)
            if nomenclature_values.get("unit") is not None:
                await check_unit_exists(nomenclature_values["unit"])

            query = (
                nomenclature.update()
                .where(nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id)
                .values(nomenclature_values)
            )
            await database.execute(query)
            nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)

        nomenclature_db = datetime_to_timestamp(nomenclature_db)

        await manager.send_message(
            token,
            {"action": "edit", "target": "nomenclature", "result": nomenclature_db},
        )

        response_body.append(nomenclature_db)

    return response_body


@router.delete("/nomenclature/{idx}/", response_model=schemas.Nomenclature)
async def delete_nomenclature(token: str, idx: int):
    """Удаление категории"""
    user = await get_user_by_token(token)

    await get_entity_by_id(nomenclature, idx, user.cashbox_id)

    query = (
        nomenclature.update()
        .where(nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = nomenclature.select().where(nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id)
    nomenclature_db = await database.fetch_one(query)
    nomenclature_db = datetime_to_timestamp(nomenclature_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "nomenclature",
            "result": nomenclature_db,
        },
    )

    return nomenclature_db


@router.delete("/nomenclature/", response_model=List[schemas.Nomenclature])
async def delete_nomenclature_mass(token: str, nomenclature_data: List[int]):
    """Удаление категории пачкой"""
    user = await get_user_by_token(token)

    response_body = []

    for idx in nomenclature_data:
        await get_entity_by_id(nomenclature, idx, user.cashbox_id)

        query = (
            nomenclature.update()
            .where(nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id)
            .values({"is_deleted": True})
        )
        await database.execute(query)

        query = nomenclature.select().where(nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id)
        nomenclature_db = await database.fetch_one(query)
        nomenclature_db = datetime_to_timestamp(nomenclature_db)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "nomenclature",
                "result": nomenclature_db,
            },
        )

        response_body.append(nomenclature_db)

    return response_body

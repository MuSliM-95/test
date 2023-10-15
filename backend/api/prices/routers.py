from typing import List, Optional

import api.prices.schemas as schemas
from database.db import categories, database, manufacturers, nomenclature, price_types, prices, units
from fastapi import APIRouter, Depends, HTTPException
from functions.filter_schemas import PricesFiltersQuery
from functions.helpers import (
    check_entity_exists,
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
    rem_owner_is_deleted,
)
from pydantic import parse_obj_as
from sqlalchemy import desc, func, select
from ws_manager import manager

router = APIRouter(tags=["prices"])


@router.get("/prices/{idx}/", response_model=schemas.Price)
async def get_price_by_id(token: str, idx: int):
    """Получение цены по ID"""
    user = await get_user_by_token(token)

    q = prices.select().where(prices.c.id == idx, prices.c.owner == user.id, prices.c.is_deleted == False)
    price_db = await database.fetch_one(q)

    if price_db:
        response_body = {**dict(price_db)}

        response_body["id"] = price_db.id
        response_body["price"] = price_db.price
        response_body["date_to"] = price_db.date_to
        response_body["date_from"] = price_db.date_from
        response_body["updated_at"] = price_db.updated_at
        response_body["created_at"] = price_db.created_at

        q = nomenclature.select().where(
            nomenclature.c.id == price_db.nomenclature,
            nomenclature.c.owner == user.id,
            nomenclature.c.is_deleted == False,
        )
        nom_db = await database.fetch_one(q)

        if price_db.price_type:
            q = price_types.select().where(price_types.c.id == price_db.price_type)
            price_type = await database.fetch_one(q)

            if price_type:
                response_body["price_type"] = price_type.name

        if nom_db:
            response_body["nomenclature_id"] = nom_db.id
            response_body["nomenclature_name"] = nom_db.name

            if nom_db.unit:
                q = units.select().where(units.c.id == nom_db.unit)
                unit = await database.fetch_one(q)

                if unit:
                    response_body["unit"] = unit.id
                    response_body["unit_name"] = unit.name

                if nom_db.category:
                    q = categories.select().where(categories.c.id == nom_db.category)
                    category = await database.fetch_one(q)

                    if category:
                        response_body["category"] = category.id
                        response_body["category_name"] = category.name

                if nom_db.manufacturer:
                    q = manufacturers.select().where(manufacturers.c.id == nom_db.manufacturer)
                    manufacturer = await database.fetch_one(q)

                    if manufacturer:
                        response_body["manufacturer"] = manufacturer.id
                        response_body["manufacturer_name"] = manufacturer.name

        response_body = datetime_to_timestamp(response_body)
        return response_body

    else:
        raise HTTPException(404, "Такой цены не найдено")


@router.get("/prices/", response_model=schemas.PriceListGet)
async def get_prices(
    token: str,
    limit: int = 100,
    offset: int = 0,
    filters: PricesFiltersQuery = Depends(),
):
    """Получение списка цен"""

    user = await get_user_by_token(token)

    filters_nom = []
    filters_price = []
    if filters.name:
        filters_nom.append(nomenclature.c.name.ilike(f"%{filters.name}%"))
    if filters.type:
        filters_nom.append(nomenclature.c.type == filters.type)
    if filters.description_short:
        filters_nom.append(nomenclature.c.description_short.ilike(f"%{filters.description_short}%"))
    if filters.description_long:
        filters_nom.append(nomenclature.c.description_long.ilike(f"%{filters.description_long}%"))
    if filters.code:
        filters_nom.append(nomenclature.c.code == filters.code)
    if filters.unit:
        filters_nom.append(nomenclature.c.unit == filters.unit)
    if filters.category:
        filters_nom.append(nomenclature.c.category.in_(filters.category.split(",")))
    if filters.manufacturer:
        filters_nom.append(nomenclature.c.manufacturer == filters.manufacturer)
    if filters.price_type_id:
        filters_price.append(prices.c.price_type == filters.price_type_id)
    if filters.date_from:
        filters_price.append(prices.c.price_type >= filters.date_from)
    if filters.date_to:
        filters_price.append(prices.c.date_to <= filters.date_to)

    if limit == -1:
        q = (
            prices.select()
            .where(prices.c.owner == user.id, prices.c.is_deleted == False, *filters_price)
            .order_by(desc(prices.c.id))
        )
        prices_db = await database.fetch_all(q)
    else:
        q = (
            prices.select()
            .where(prices.c.owner == user.id, prices.c.is_deleted == False, *filters_price)
            .order_by(desc(prices.c.id))
            .limit(limit)
            .offset(offset)
        )
        prices_db = await database.fetch_all(q)

    response_body_list = []

    for price_db in prices_db:
        response_body = {**dict(price_db)}

        response_body["id"] = price_db.id
        response_body["price"] = price_db.price
        response_body["date_to"] = price_db.date_to
        response_body["date_from"] = price_db.date_from
        response_body["updated_at"] = price_db.updated_at
        response_body["created_at"] = price_db.created_at

        q = nomenclature.select().where(
            nomenclature.c.id == price_db.nomenclature,
            nomenclature.c.owner == user.id,
            nomenclature.c.is_deleted == False,
            *filters_nom,
        )
        nom_db = await database.fetch_one(q)

        if price_db.price_type:
            q = price_types.select().where(price_types.c.id == price_db.price_type)
            price_type = await database.fetch_one(q)

            if price_type:
                response_body["price_type"] = price_type.name

        if nom_db:
            response_body["nomenclature_id"] = nom_db.id
            response_body["nomenclature_name"] = nom_db.name

            if nom_db.unit:
                q = units.select().where(units.c.id == nom_db.unit)
                unit = await database.fetch_one(q)

                if unit:
                    response_body["unit"] = unit.id
                    response_body["unit_name"] = unit.name

                if nom_db.category:
                    q = categories.select().where(categories.c.id == nom_db.category)
                    category = await database.fetch_one(q)

                    if category:
                        response_body["category"] = category.id
                        response_body["category_name"] = category.name

                if nom_db.manufacturer:
                    q = manufacturers.select().where(manufacturers.c.id == nom_db.manufacturer)
                    manufacturer = await database.fetch_one(q)

                    if manufacturer:
                        response_body["manufacturer"] = manufacturer.id
                        response_body["manufacturer_name"] = manufacturer.name

        else:
            continue

        response_body = datetime_to_timestamp(response_body)
        response_body_list.append(response_body)

    q = select(func.count(prices.c.id)).where(prices.c.owner == user.id, prices.c.is_deleted == False, *filters_price)
    prices_db_count = await database.fetch_one(q)

    return {"result": response_body_list, "count": prices_db_count.count_1}


@router.post("/prices/", response_model=schemas.PriceList)
async def new_price(token: str, prices_data: schemas.PriceCreateMass):
    """Создание цен"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    price_types_cache = set()
    nomenclature_cache = set()
    exceptions = []
    for price_values in prices_data.dict()["__root__"]:
        price_values["owner"] = user.id
        price_values["is_deleted"] = False
        price_values["cashbox"] = user.cashbox_id

        if price_values.get("price_type") is not None:
            if price_values["price_type"] not in price_types_cache:
                try:
                    await check_entity_exists(price_types, price_values["price_type"], user.id)
                    price_types_cache.add(price_values["price_type"])
                except HTTPException as e:
                    exceptions.append(str(price_values) + " " + e.detail)
                    continue

        if price_values.get("nomenclature") is not None:
            if price_values["nomenclature"] not in nomenclature_cache:
                try:
                    await check_entity_exists(nomenclature, price_values["nomenclature"], user.id)
                    nomenclature_cache.add(price_values["nomenclature"])
                except HTTPException as e:
                    exceptions.append(str(price_values) + " " + e.detail)
                    continue

        # if price_values.get("price_type") is not None and price_values.get("nomenclature") is not None:
        #     q = prices.select().where(prices.c.owner == user.id, prices.c.is_deleted == False, prices.c.nomenclature == price_values['nomenclature'], prices.c.price_type == price_values['price_type'])
        #     ex_price = await database.fetch_one(q)
        #     if ex_price:
        #         raise HTTPException(403, "Цена с таким типом уже существует")

        query = prices.insert().values(price_values)
        price_id = await database.execute(query)
        inserted_ids.add(price_id)

    query = prices.select().where(prices.c.owner == user.id, prices.c.id.in_(inserted_ids))
    prices_db = await database.fetch_all(query)
    # prices_db = [*map(datetime_to_timestamp, prices_db)]
    # prices_db = [*map(rem_owner_is_deleted, prices_db)]

    response_body_list = []

    for price_db in prices_db:
        response_body = {**dict(price_db)}

        response_body["id"] = price_db.id
        response_body["price"] = price_db.price
        response_body["date_to"] = price_db.date_to
        response_body["date_from"] = price_db.date_from
        response_body["updated_at"] = price_db.updated_at
        response_body["created_at"] = price_db.created_at

        q = nomenclature.select().where(
            nomenclature.c.id == price_db.nomenclature,
            nomenclature.c.owner == user.id,
            nomenclature.c.is_deleted == False,
        )
        nom_db = await database.fetch_one(q)

        if price_db.price_type:
            q = price_types.select().where(price_types.c.id == price_db.price_type)
            price_type = await database.fetch_one(q)

            if price_type:
                response_body["price_type"] = price_type.name

        if nom_db:
            response_body["nomenclature_id"] = nom_db.id
            response_body["nomenclature_name"] = nom_db.name

            if nom_db.unit:
                q = units.select().where(units.c.id == nom_db.unit)
                unit = await database.fetch_one(q)

                if unit:
                    response_body["unit"] = unit.id
                    response_body["unit_name"] = unit.name

                if nom_db.category:
                    q = categories.select().where(categories.c.id == nom_db.category)
                    category = await database.fetch_one(q)

                    if category:
                        response_body["category"] = category.id
                        response_body["category_name"] = category.name

                if nom_db.manufacturer:
                    q = manufacturers.select().where(manufacturers.c.id == nom_db.manufacturer)
                    manufacturer = await database.fetch_one(q)

                    if manufacturer:
                        response_body["manufacturer"] = manufacturer.id
                        response_body["manufacturer_name"] = manufacturer.name

        response_body = datetime_to_timestamp(response_body)
        response_body_list.append(response_body)

    websocket_body = parse_obj_as(Optional[List[schemas.PriceInList]], response_body_list)
    websocket_body = [body.dict() for body in websocket_body]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "prices",
            "result": websocket_body,
        },
    )

    if exceptions:
        raise HTTPException(400, "Не были добавлены следующие записи: " + ", ".join(exceptions))

    return response_body_list


@router.patch("/prices/{idx}/", response_model=schemas.PriceInList)
async def edit_price(
    token: str,
    idx: int,
    price: schemas.PriceEditOne,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
):
    """Редактирование цены"""
    user = await get_user_by_token(token)

    dates_filters = []
    if date_from and not date_to:
        dates_filters.append(prices.c.date_from <= date_from)
    if not date_from and date_to:
        dates_filters.append(prices.c.date_to <= date_to)
    if date_from and date_to:
        dates_filters.append(prices.c.date_from <= date_from, prices.c.date_to <= date_to)

    q = prices.select().where(prices.c.id == idx, *dates_filters)
    price_db = await database.fetch_one(q)

    price_db = await get_entity_by_id(prices, price_db.id, user.id)
    price_values = price.dict(exclude_unset=True)

    if price_values:
        # if price_values.get("price_type") is not None and price_values.get("nomenclature") is not None:
        #     q = prices.select().where(prices.c.owner == user.id, prices.c.is_deleted == False, prices.c.nomenclature == price_values['nomenclature'], prices.c.price_type == price_values['price_type'])
        #     ex_price = await database.fetch_one(q)
        #     if ex_price:
        #         raise HTTPException(403, "Цена с таким типом уже существует")

        if price_values.get("price_type") is not None:
            await get_entity_by_id(price_types, price_values["price_type"], user.id)

        query = prices.update().where(prices.c.id == idx, prices.c.owner == user.id).values(price_values)
        await database.execute(query)
        price_db = await get_entity_by_id(prices, price_db.id, user.id)

    if price_db:
        response_body = {**dict(price_db)}

        response_body["id"] = price_db.id
        response_body["price"] = price_db.price
        response_body["date_to"] = price_db.date_to
        response_body["date_from"] = price_db.date_from
        response_body["updated_at"] = price_db.updated_at
        response_body["created_at"] = price_db.created_at

        q = nomenclature.select().where(
            nomenclature.c.id == price_db.nomenclature,
            nomenclature.c.owner == user.id,
            nomenclature.c.is_deleted == False,
        )
        nom_db = await database.fetch_one(q)

        if price_db.price_type:
            q = price_types.select().where(price_types.c.id == price_db.price_type)
            price_type = await database.fetch_one(q)

            if price_type:
                response_body["price_type"] = price_type.name

        if nom_db:
            response_body["nomenclature_id"] = nom_db.id
            response_body["nomenclature_name"] = nom_db.name

            if nom_db.unit:
                q = units.select().where(units.c.id == nom_db.unit)
                unit = await database.fetch_one(q)

                if unit:
                    response_body["unit"] = unit.id
                    response_body["unit_name"] = unit.name

                if nom_db.category:
                    q = categories.select().where(categories.c.id == nom_db.category)
                    category = await database.fetch_one(q)

                    if category:
                        response_body["category"] = category.id
                        response_body["category_name"] = category.name

                if nom_db.manufacturer:
                    q = manufacturers.select().where(manufacturers.c.id == nom_db.manufacturer)
                    manufacturer = await database.fetch_one(q)

                    if manufacturer:
                        response_body["manufacturer"] = manufacturer.id
                        response_body["manufacturer_name"] = manufacturer.name

        response_body = datetime_to_timestamp(response_body)

        websocket_body = parse_obj_as(schemas.PriceInList, response_body).dict()

        await manager.send_message(
            token,
            {"action": "edit", "target": "prices", "result": websocket_body},
        )

        return response_body


@router.patch("/prices/", response_model=schemas.PriceList)
async def edit_price(
    token: str,
    prices_list: List[schemas.PriceEdit],
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
):
    """Редактирование цены пачкой"""
    user = await get_user_by_token(token)
    response_body_list = []
    for price in prices_list:

        dates_filters = []
        if date_from and not date_to:
            dates_filters.append(prices.c.date_from <= date_from)
        if not date_from and date_to:
            dates_filters.append(prices.c.date_to <= date_to)
        if date_from and date_to:
            dates_filters.append(prices.c.date_from <= date_from, prices.c.date_to <= date_to)

        q = prices.select().where(prices.c.id == price.id, *dates_filters)
        price_db = await database.fetch_one(q)

        price_values = price.dict(exclude_unset=True)

        if price_values:
            if price_values.get("price_type") is not None:
                await get_entity_by_id(price_types, price_values["price_type"], user.id)
            if price_values.get("nomenclature") is not None:
                await get_entity_by_id(nomenclature, price_values["nomenclature"], user.id)

            # if price_values.get("price_type") is not None and price_values.get("nomenclature") is not None:
            #     q = prices.select().where(prices.c.owner == user.id, prices.c.is_deleted == False, prices.c.nomenclature == price_values['nomenclature'], prices.c.price_type == price_values['price_type'])
            #     ex_price = await database.fetch_one(q)
            #     if ex_price:
            #         raise HTTPException(403, "Цена с таким типом уже существует")

            query = prices.update().where(prices.c.id == price_db.id, prices.c.owner == user.id).values(price_values)
            await database.execute(query)
            price_db = await get_entity_by_id(prices, price_db.id, user.id)

        response_body = {**dict(price_db)}

        response_body["id"] = price_db.id
        response_body["price"] = price_db.price
        response_body["date_to"] = price_db.date_to
        response_body["date_from"] = price_db.date_from
        response_body["updated_at"] = price_db.updated_at
        response_body["created_at"] = price_db.created_at

        q = nomenclature.select().where(
            nomenclature.c.id == price_db.nomenclature,
            nomenclature.c.owner == user.id,
            nomenclature.c.is_deleted == False,
        )
        nom_db = await database.fetch_one(q)

        if price_db.price_type:
            q = price_types.select().where(price_types.c.id == price_db.price_type)
            price_type = await database.fetch_one(q)

            if price_type:
                response_body["price_type"] = price_type.name

        if nom_db:
            response_body["nomenclature_id"] = nom_db.id
            response_body["nomenclature_name"] = nom_db.name

            if nom_db.unit:
                q = units.select().where(units.c.id == nom_db.unit)
                unit = await database.fetch_one(q)

                if unit:
                    response_body["unit"] = unit.id
                    response_body["unit_name"] = unit.name

                if nom_db.category:
                    q = categories.select().where(categories.c.id == nom_db.category)
                    category = await database.fetch_one(q)

                    if category:
                        response_body["category"] = category.id
                        response_body["category_name"] = category.name

                if nom_db.manufacturer:
                    q = manufacturers.select().where(manufacturers.c.id == nom_db.manufacturer)
                    manufacturer = await database.fetch_one(q)

                    if manufacturer:
                        response_body["manufacturer"] = manufacturer.id
                        response_body["manufacturer_name"] = manufacturer.name

        response_body = datetime_to_timestamp(response_body)
        response_body_list.append(response_body)

    websocket_body = parse_obj_as(Optional[List[schemas.PriceInList]], response_body_list)
    websocket_body = [body.dict() for body in websocket_body]

    await manager.send_message(
        token,
        {"action": "edit", "target": "prices", "result": websocket_body},
    )

    return response_body_list


@router.delete("/prices/{idx}/", response_model=schemas.PriceInList)
async def delete_price(token: str, idx: int, date_from: Optional[int] = None, date_to: Optional[int] = None):
    """Удаление цены"""
    user = await get_user_by_token(token)

    dates_filters = []
    if date_from and not date_to:
        dates_filters.append(prices.c.date_from <= date_from)
    if not date_from and date_to:
        dates_filters.append(prices.c.date_to <= date_to)
    if date_from and date_to:
        dates_filters.append(prices.c.date_from <= date_from, prices.c.date_to <= date_to)

    await get_entity_by_id(prices, idx, user.id)

    query = prices.select().where(prices.c.id == idx, prices.c.owner == user.id, prices.c.is_deleted == False)
    price_db = await database.fetch_one(query)

    query = prices.update().where(prices.c.id == idx, prices.c.owner == user.id).values({"is_deleted": True})
    await database.execute(query)

    response_body = {**dict(price_db)}

    response_body["id"] = price_db.id
    response_body["price"] = price_db.price
    response_body["date_to"] = price_db.date_to
    response_body["date_from"] = price_db.date_from
    response_body["updated_at"] = price_db.updated_at
    response_body["created_at"] = price_db.created_at

    q = nomenclature.select().where(
        nomenclature.c.id == price_db.nomenclature, nomenclature.c.owner == user.id, nomenclature.c.is_deleted == False
    )
    nom_db = await database.fetch_one(q)

    if price_db.price_type:
        q = price_types.select().where(price_types.c.id == price_db.price_type)
        price_type = await database.fetch_one(q)

        if price_type:
            response_body["price_type"] = price_type.name

    if nom_db:
        response_body["nomenclature_id"] = nom_db.id
        response_body["nomenclature_name"] = nom_db.name

        if nom_db.unit:
            q = units.select().where(units.c.id == nom_db.unit)
            unit = await database.fetch_one(q)

            if unit:
                response_body["unit"] = unit.id
                response_body["unit_name"] = unit.name

            if nom_db.category:
                q = categories.select().where(categories.c.id == nom_db.category)
                category = await database.fetch_one(q)

                if category:
                    response_body["category"] = category.id
                    response_body["category_name"] = category.name

            if nom_db.manufacturer:
                q = manufacturers.select().where(manufacturers.c.id == nom_db.manufacturer)
                manufacturer = await database.fetch_one(q)

                if manufacturer:
                    response_body["manufacturer"] = manufacturer.id
                    response_body["manufacturer_name"] = manufacturer.name

    response_body = datetime_to_timestamp(response_body)

    websocket_body = parse_obj_as(schemas.PriceInList, response_body).dict()

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "prices",
            "result": websocket_body,
        },
    )

    return response_body


@router.delete("/prices/", response_model=schemas.PriceList)
async def delete_price_mass(token: str, ids: str, date_from: Optional[int] = None, date_to: Optional[int] = None):
    """Удаление цены пачкой"""
    user = await get_user_by_token(token)

    response_body_list = []

    for price_id in ids.split(","):
        dates_filters = []
        if date_from and not date_to:
            dates_filters.append(prices.c.date_from <= date_from)
        if not date_from and date_to:
            dates_filters.append(prices.c.date_to <= date_to)
        if date_from and date_to:
            dates_filters.append(prices.c.date_from <= date_from, prices.c.date_to <= date_to)

        await get_entity_by_id(prices, int(price_id), user.id)

        query = (
            prices.update().where(prices.c.id == int(price_id), prices.c.owner == user.id).values({"is_deleted": True})
        )
        await database.execute(query)

        query = prices.select().where(prices.c.id == int(price_id), prices.c.owner == user.id)
        price_db = await database.fetch_one(query)

        response_body = {**dict(price_db)}

        response_body["id"] = price_db.id
        response_body["price"] = price_db.price
        response_body["date_to"] = price_db.date_to
        response_body["date_from"] = price_db.date_from
        response_body["updated_at"] = price_db.updated_at
        response_body["created_at"] = price_db.created_at

        q = nomenclature.select().where(
            nomenclature.c.id == price_db.nomenclature,
            nomenclature.c.owner == user.id,
            nomenclature.c.is_deleted == False,
        )
        nom_db = await database.fetch_one(q)

        if price_db.price_type:
            q = price_types.select().where(price_types.c.id == price_db.price_type)
            price_type = await database.fetch_one(q)

            if price_type:
                response_body["price_type"] = price_type.name

        if nom_db:
            response_body["nomenclature_id"] = nom_db.id
            response_body["nomenclature_name"] = nom_db.name

            if nom_db.unit:
                q = units.select().where(units.c.id == nom_db.unit)
                unit = await database.fetch_one(q)

                if unit:
                    response_body["unit"] = unit.id
                    response_body["unit_name"] = unit.name

                if nom_db.category:
                    q = categories.select().where(categories.c.id == nom_db.category)
                    category = await database.fetch_one(q)

                    if category:
                        response_body["category"] = category.id
                        response_body["category_name"] = category.name

                if nom_db.manufacturer:
                    q = manufacturers.select().where(manufacturers.c.id == nom_db.manufacturer)
                    manufacturer = await database.fetch_one(q)

                    if manufacturer:
                        response_body["manufacturer"] = manufacturer.id
                        response_body["manufacturer_name"] = manufacturer.name

        response_body = datetime_to_timestamp(response_body)
        response_body_list.append(response_body)

    websocket_body = parse_obj_as(Optional[List[schemas.PriceInList]], response_body_list)
    websocket_body = [body.dict() for body in websocket_body]

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "prices",
            "result": websocket_body,
        },
    )

    return response_body_list

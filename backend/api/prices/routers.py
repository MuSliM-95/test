from fastapi import APIRouter, HTTPException, Depends

from database.db import database, prices, price_types, nomenclature

import api.prices.schemas as schemas
from functions.filter_schemas import PricesFiltersQuery

from functions.helpers import (
    datetime_to_timestamp,
    get_entity_by_id,
    check_entity_exists,
)
from functions.helpers import get_user_by_token

from ws_manager import manager

router = APIRouter(tags=["prices"])


@router.get("/prices/{idx}", response_model=schemas.Price)
async def get_price_by_id(token: str, idx: int):
    """Получение цены по ID"""
    user = await get_user_by_token(token)
    query = """
    select p.id, n.name, n.type, n.description_short, n.description_long, n.code,
    n.unit, u.name as unit_name, n.category, c.name as category_name,
    n.manufacturer, m.name as manufacturer_name, price,
    pt.name as price_type, p.date_to as price_finishes, p.updated_at, p.created_at
    from prices p
    join nomenclature n ON (p.nomenclature = n.id)
    left outer join units u on (n.unit = u.id)
    left outer join categories c on n.category = c.id
    left outer join manufacturers m on n.manufacturer = m.id
    left outer join price_types pt on p.price_type = pt.id
    where n.id = :idx and p.owner = :owner and n.owner = :owner
    """

    price_db = await database.fetch_one(query, {"owner": user.id, "idx": idx})
    price_db = datetime_to_timestamp(price_db)
    return price_db


@router.get("/prices/", response_model=schemas.PriceList)
async def get_prices(
    token: str,
    limit: int = 100,
    offset: int = 0,
    filters: PricesFiltersQuery = Depends(),
):
    """Получение списка цен"""

    user = await get_user_by_token(token)

    filters_sql = ""
    if filters.name:
        filters_sql += f"and n.name ilike '{filters.name}%' "
    if filters.type:
        filters_sql += f"and n.type = '{filters.type}' "
    if filters.description_short:
        filters_sql += f"and n.description_short ilike '%{filters.description_short}%' "
    if filters.description_long:
        filters_sql += f"and n.description_long ilike '%{filters.description_long}%' "
    if filters.code:
        filters_sql += f"and n.code = {filters.code} "
    if filters.unit:
        filters_sql += f"and n.unit = {filters.unit} "
    if filters.category:
        filters_sql += f"and n.category = {filters.category} "
    if filters.manufacturer:
        filters_sql += f"and n.manufacturer = {filters.manufacturer} "
    if filters.price_type_id:
        filters_sql += f"and p.price_type = {filters.price_type_id} "
    if filters.date_from:
        filters_sql += f"and p.date_from >= {filters.date_from} "
    if filters.date_to:
        filters_sql += f"and p.date_to <= {filters.date_to}"

    query = f"""
    select p.id, n.name, n.type, n.description_short, n.description_long, n.code,
    n.unit, u.name as unit_name, n.category, c.name as category_name,
    n.manufacturer, m.name as manufacturer_name, price,
    pt.name as price_type, p.date_to as price_finishes, p.updated_at, p.created_at
    from prices p
    join nomenclature n on (p.nomenclature = n.id)
    left outer join units u on (n.unit = u.id)
    left outer join categories c on n.category = c.id
    left outer join manufacturers m on n.manufacturer = m.id
    left outer join price_types pt on p.price_type = pt.id
    where p.owner = :owner and n.owner = :owner
    {filters_sql}
    limit :limit
    offset :offset
    """

    values = {"limit": limit, "offset": offset, "owner": user.id}

    prices_db = await database.fetch_all(query, values)
    prices_db = [*map(datetime_to_timestamp, prices_db)]

    return prices_db


@router.post("/prices/", response_model=schemas.PriceListPure)
async def new_price(token: str, prices_data: schemas.PriceCreateMass):
    """Создание цен"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    price_types_cache = set()
    nomenclature_cache = set()
    exceptions = []
    for price_values in prices_data.dict()["__root__"]:
        price_values["owner"] = user.id

        if price_values.get("price_type") is not None:
            if price_values["price_type"] not in price_types_cache:
                try:
                    await check_entity_exists(
                        price_types, price_values["price_type"], user.id
                    )
                    price_types_cache.add(price_values["price_type"])
                except HTTPException as e:
                    exceptions.append(str(price_values) + " " + e.detail)
                    continue

        if price_values.get("nomenclature") is not None:
            if price_values["nomenclature"] not in nomenclature_cache:
                try:
                    await check_entity_exists(
                        nomenclature, price_values["nomenclature"], user.id
                    )
                    nomenclature_cache.add(price_values["nomenclature"])
                except HTTPException as e:
                    exceptions.append(str(price_values) + " " + e.detail)
                    continue

        query = prices.insert().values(price_values)
        price_id = await database.execute(query)
        inserted_ids.add(price_id)

    query = prices.select().where(
        prices.c.owner == user.id, prices.c.id.in_(inserted_ids)
    )
    prices_db = await database.fetch_all(query)
    prices_db = [*map(datetime_to_timestamp, prices_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "prices",
            "result": prices_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return prices_db


@router.patch("/prices/{idx}", response_model=schemas.Price)
async def edit_price(
    token: str,
    idx: int,
    price: schemas.PriceEdit,
):
    """Редактирование цены"""
    user = await get_user_by_token(token)
    price_db = await get_entity_by_id(prices, idx, user.id)
    price_values = price.dict(exclude_unset=True)

    if price_values:
        if price_values.get("price_type") is not None:
            await get_entity_by_id(price_types, price_values["price_type"], user.id)
        if price_values.get("nomenclature") is not None:
            await get_entity_by_id(nomenclature, price_values["nomenclature"], user.id)

        query = (
            prices.update()
            .where(prices.c.id == idx, prices.c.owner == user.id)
            .values(price_values)
        )
        await database.execute(query)
        price_db = await get_entity_by_id(prices, idx, user.id)

    price_db = datetime_to_timestamp(price_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "prices", "result": price_db},
    )

    return price_db


@router.delete("/prices/{idx}", response_model=schemas.Price)
async def delete_price(token: str, idx: int):
    """Удаление цены"""
    user = await get_user_by_token(token)

    await get_entity_by_id(prices, idx, user.id)

    query = (
        prices.update()
        .where(prices.c.id == idx, prices.c.owner == user.id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = prices.select().where(prices.c.id == idx, prices.c.owner == user.id)
    price_db = await database.fetch_one(query)
    price_db = datetime_to_timestamp(price_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "prices",
            "result": price_db,
        },
    )

    return price_db

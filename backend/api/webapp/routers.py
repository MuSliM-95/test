from functions.filter_schemas import PicturesFiltersQuery
from database.db import categories, database, manufacturers, nomenclature, price_types, prices, units
from functions.filter_schemas import PricesFiltersQuery
from sqlalchemy import desc, case
from database.db import database, warehouse_balances, warehouses, warehouse_register_movement, nomenclature, \
    OperationType, organizations
from typing import Optional
import api.nomenclature.schemas as schemas
from database.db import categories, database, manufacturers, nomenclature, pictures
from fastapi import APIRouter, Depends
from functions.helpers import (
    check_entity_exists,
    check_unit_exists,
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
    nomenclature_unit_id_to_name,
)
from sqlalchemy import func, select

router = APIRouter(tags=["webapp"])


@router.get("/webapp/", response_model=schemas.NomenclatureListGetRes)
async def get_nomenclature(
        token: str,
        warehouse_id: Optional[int] = None,
        nomenclature_id: Optional[int] = None,
        organization_id: Optional[int] = None,
        name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        filter_pictures: PicturesFiltersQuery = Depends(),
        filter_prices: PricesFiltersQuery = Depends(),
):
    """Получение категорий, фотографий, цен, остатков"""
    user = await get_user_by_token(token)

    #  category names

    filters = [
        nomenclature.c.owner == user.id,
        nomenclature.c.is_deleted.is_not(True),
    ]

    if name:
        filters.append(nomenclature.c.name.ilike(f"%{name}%"))

    query = nomenclature.select().where(*filters).limit(limit).offset(offset)

    nomenclature_db = await database.fetch_all(query)
    nomenclature_db = [*map(datetime_to_timestamp, nomenclature_db)]
    nomenclature_db = [*map(nomenclature_unit_id_to_name, nomenclature_db)]
    nomenclature_db = [await inst for inst in nomenclature_db]

    query = select(func.count(nomenclature.c.id)).where(*filters)
    nomenclature_db_c = await database.fetch_one(query)

    #  pictures

    filters_list = []
    if filter_pictures.entity:
        filters_list.append(pictures.c.entity == filter_pictures.entity)
    if filter_pictures.entity_id:
        filters_list.append(pictures.c.entity_id == filter_pictures.entity_id)

    query = (
        pictures.select()
        .where(
            pictures.c.owner == user.id,
            pictures.c.is_deleted.is_not(True),
            *filters_list,
        )
        .limit(limit)
        .offset(offset)
    )

    pictures_db = await database.fetch_all(query)
    pictures_db = [*map(datetime_to_timestamp, pictures_db)]

    query = (
        select(func.count(pictures.c.id))
        .where(
            pictures.c.owner == user.id,
            pictures.c.is_deleted.is_not(True),
            *filters_list,
        )
    )

    pictures_db_c = await database.fetch_one(query)

    #  price_types

    query = (
        price_types.select()
        .where(
            price_types.c.owner == user.id,
            price_types.c.is_deleted.is_not(True),
        )
        .limit(limit)
        .offset(offset)
    )

    price_types_db = await database.fetch_all(query)
    price_types_db = [*map(datetime_to_timestamp, price_types_db)]

    query = (
        select(func.count(price_types.c.id))
        .where(
            price_types.c.owner == user.id,
            price_types.c.is_deleted.is_not(True),
        )
    )

    price_types_db_count = await database.fetch_one(query)

    #  prices

    filters_nom = []
    filters_price = []
    if filter_prices.name:
        filters_nom.append(nomenclature.c.name.ilike(f"%{filters.name}%"))
    if filter_prices.type:
        filters_nom.append(nomenclature.c.type == filter_prices.type)
    if filter_prices.description_short:
        filters_nom.append(nomenclature.c.description_short.ilike(f"%{filters.description_short}%"))
    if filter_prices.description_long:
        filters_nom.append(nomenclature.c.description_long.ilike(f"%{filters.description_long}%"))
    if filter_prices.code:
        filters_nom.append(nomenclature.c.code == filter_prices.code)
    if filter_prices.unit:
        filters_nom.append(nomenclature.c.unit == filters.unit)
    if filter_prices.category:
        filters_nom.append(nomenclature.c.category.in_(filter_prices.category.split(",")))
    if filter_prices.manufacturer:
        filters_nom.append(nomenclature.c.manufacturer == filter_prices.manufacturer)
    if filter_prices.price_type_id:
        filters_price.append(prices.c.price_type == filter_prices.price_type_id)
    if filter_prices.date_from:
        filters_price.append(prices.c.price_type >= filter_prices.date_from)
    if filter_prices.date_to:
        filters_price.append(prices.c.date_to <= filter_prices.date_to)

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

    #  remains(ost) xd

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
    ) \
        .select_from(warehouse_register_movement
                     .join(nomenclature,
                           warehouse_register_movement.c.nomenclature_id == nomenclature.c.id
                           ))

    warehouse_balances_db = await database.fetch_all(query)
    # warehouse_balances_db = [*map(datetime_to_timestamp, warehouse_balances_db)]
    res = []
    for warehouse_balance in warehouse_balances_db:
        balance_dict = dict(warehouse_balance)
        organization_db = await database.fetch_one(
            organizations.select().where(organizations.c.id == warehouse_balance.organization_id))
        warehouse_db = await database.fetch_one(
            warehouses.select().where(warehouses.c.id == warehouse_balance.warehouse_id))
        balance_dict['organization_name'] = organization_db.short_name
        balance_dict['warehouse_name'] = warehouse_db.name

        res.append(balance_dict)


    for itemitem in nomenclature_db:
        query = pictures.select().where(pictures.c.entity_id == itemitem['id'])
        pictures_db = await database.fetch_all(query)
        itemitem['pictures'] = pictures_db


    return {"result": nomenclature_db, "count": nomenclature_db_c.count_1}

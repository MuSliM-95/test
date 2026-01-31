import logging
from typing import List, Optional

import api.prices.schemas as schemas
from common.geocoders.instance import geocoder
from database.db import (
    categories,
    database,
    manufacturers,
    nomenclature,
    pictures,
    price_types,
    prices,
    units,
)
from fastapi import APIRouter, Depends, HTTPException, Query
from functions.filter_schemas import PricesFiltersQuery
from functions.helpers import (
    check_entity_exists,
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
    raise_bad_request,
)
from pydantic import parse_obj_as
from sqlalchemy import and_, desc, func, select
from ws_manager import manager

router = APIRouter(tags=["prices"])


@router.get("/prices/{idx}/", response_model=schemas.Price)
async def get_price_by_id(token: str, idx: int):
    """Получение цены по ID"""
    user = await get_user_by_token(token)

    q = prices.select().where(
        prices.c.id == idx,
        prices.c.cashbox == user.cashbox_id,
        prices.c.is_deleted == False,
    )
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
            nomenclature.c.cashbox == user.cashbox_id,
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
                    q = manufacturers.select().where(
                        manufacturers.c.id == nom_db.manufacturer
                    )
                    manufacturer = await database.fetch_one(q)

                    if manufacturer:
                        response_body["manufacturer"] = manufacturer.id
                        response_body["manufacturer_name"] = manufacturer.name

        response_body = datetime_to_timestamp(response_body)
        return response_body

    else:
        raise HTTPException(404, "Такой цены не найдено")


@router.get("/alt_prices/{idx}/", response_model=schemas.Price)
async def get_price_by_id(
    token: str, idx: int, filters: schemas.FilterSchema = Depends()
):
    """Получение цены по ID номенклатуры"""
    user = await get_user_by_token(token)

    q = nomenclature.select().where(
        nomenclature.c.id == idx,
        nomenclature.c.cashbox == user.cashbox_id,
        nomenclature.c.is_deleted == False,
    )
    nom_db = await database.fetch_one(q)

    if nom_db:
        q = prices.select().where(
            prices.c.nomenclature == nom_db.id,
            prices.c.cashbox == user.cashbox_id,
            prices.c.is_deleted == False,
        )

        if filters.price_type_id:
            q = q.where(prices.c.price_type == filters.price_type_id)

        price_db = await database.fetch_one(q)

        if price_db:
            response_body = {**dict(price_db)}

            response_body["id"] = price_db.id
            response_body["price"] = price_db.price
            response_body["date_to"] = price_db.date_to
            response_body["date_from"] = price_db.date_from
            response_body["updated_at"] = price_db.updated_at
            response_body["created_at"] = price_db.created_at

            if price_db.price_type:
                q = price_types.select().where(price_types.c.id == price_db.price_type)
                price_type = await database.fetch_one(q)

                if price_type:
                    response_body["price_type"] = price_type.name

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
                    q = manufacturers.select().where(
                        manufacturers.c.id == nom_db.manufacturer
                    )
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
    page: int = 1,
    limit: int = 100,
    offset: int = 0,
    filters: PricesFiltersQuery = Depends(),
    with_photos: bool = Query(False, description="Включить фото номенклатуры в ответ"),
):
    """Получение списка цен"""

    user = await get_user_by_token(token)

    filters_nom = []
    filters_price = []
    filters_price_type = []
    if filters.name:
        filters_nom.append(nomenclature.c.name.ilike(f"%{filters.name}%"))
    if filters.type:
        filters_nom.append(nomenclature.c.type == filters.type)
    if filters.description_short:
        filters_nom.append(
            nomenclature.c.description_short.ilike(f"%{filters.description_short}%")
        )
    if filters.description_long:
        filters_nom.append(
            nomenclature.c.description_long.ilike(f"%{filters.description_long}%")
        )
    if filters.code:
        filters_nom.append(nomenclature.c.code == filters.code)
    if filters.unit:
        filters_nom.append(nomenclature.c.unit == filters.unit)
    if filters.category_ids:
        category_ids = []
        try:
            category_ids = [int(ix) for ix in filters.category_ids.split(",")]
        except ValueError:
            raise_bad_request("Category IDs must contain only numbers")
        filters_nom.append(nomenclature.c.category.in_(category_ids))
    if filters.manufacturer:
        filters_nom.append(nomenclature.c.manufacturer == filters.manufacturer)
    if filters.price_type_id:
        filters_price.append(prices.c.price_type == filters.price_type_id)
    if filters.date_from:
        filters_price.append(prices.c.date_from >= filters.date_from)
    if filters.date_to:
        filters_price.append(prices.c.date_to <= filters.date_to)

    if filters.price_type_tags:
        tag_list = [tag.strip() for tag in filters.price_type_tags.split(",")]

        if filters.price_type_tags_mode not in ("and", "or"):
            raise_bad_request("price_type_tags_mode must be 'and' or 'or'")

        if filters.price_type_tags_mode == "and":
            filters_price_type.append(price_types.c.tags.contains(tag_list))
        else:
            filters_price_type.append(price_types.c.tags.overlap(tag_list))

    if limit == -1:
        q = (
            prices.select()
            .where(
                prices.c.cashbox == user.cashbox_id,
                prices.c.is_deleted == False,
                *filters_price,
            )
            .order_by(desc(prices.c.id))
        )
        prices_db = await database.fetch_all(q)
    else:
        q = (
            select(
                prices.c.id,
                nomenclature.c.id.label("nomenclature_id"),
                nomenclature.c.name.label("nomenclature_name"),
                price_types.c.name.label("price_type"),
                nomenclature.c.description_short,
                nomenclature.c.description_long,
                nomenclature.c.code,
                nomenclature.c.unit,
                units.c.name.label("unit_name"),
                nomenclature.c.category,
                categories.c.name.label("category_name"),
                nomenclature.c.manufacturer,
                manufacturers.c.name.label("manufacturer_name"),
                prices.c.price,
                prices.c.date_to,
                prices.c.date_from,
                prices.c.warehouse_id,
            )
            .join(nomenclature, nomenclature.c.id == prices.c.nomenclature)
            .join(units, units.c.id == nomenclature.c.unit, full=True)
            .join(categories, categories.c.id == nomenclature.c.category, full=True)
            .join(
                manufacturers,
                manufacturers.c.id == nomenclature.c.manufacturer,
                full=True,
            )
            .join(price_types, price_types.c.id == prices.c.price_type, full=True)
            .where(
                prices.c.cashbox == user.cashbox_id,
                prices.c.is_deleted == False,
                nomenclature.c.is_deleted == False,
                *filters_price,
                *filters_nom,
                *filters_price_type,
            )
            .order_by(desc(prices.c.id))
            .limit(limit)
            .offset((page - 1) * limit)
        )
        print(q)
        prices_db = await database.fetch_all(q)

    # Если нужны фото, загружаем их для номенклатур
    if with_photos and prices_db:
        # Преобразуем Row объекты в dict для модификации
        prices_db = [dict(price) for price in prices_db]

        # Собираем уникальные ID номенклатур (пропускаем None)
        nomenclature_ids = list(
            set(
                [
                    price["nomenclature_id"]
                    for price in prices_db
                    if price.get("nomenclature_id") is not None
                ]
            )
        )

        if nomenclature_ids:
            # Загружаем все фото одним запросом
            photos_query = (
                select(
                    pictures.c.entity_id.label("nomenclature_id"),
                    pictures.c.id,
                    pictures.c.url,
                    pictures.c.is_main,
                    pictures.c.created_at,
                    pictures.c.updated_at,
                )
                .select_from(pictures)
                .where(
                    pictures.c.entity == "nomenclature",
                    pictures.c.entity_id.in_(nomenclature_ids),
                    pictures.c.is_deleted.is_not(True),
                )
                .order_by(
                    pictures.c.entity_id, pictures.c.is_main.desc(), pictures.c.id.asc()
                )
            )
            photos_list = await database.fetch_all(photos_query)

            # Группируем фото по nomenclature_id
            photos_dict = {}
            for photo in photos_list:
                nom_id = photo["nomenclature_id"]
                if nom_id not in photos_dict:
                    photos_dict[nom_id] = []
                photo_dict = dict(photo)
                del photo_dict["nomenclature_id"]
                photos_dict[nom_id].append(datetime_to_timestamp(photo_dict))

            # Добавляем фото к ценам
            for price in prices_db:
                nom_id = price.get("nomenclature_id")
                price["photos"] = photos_dict.get(nom_id, [])
        else:
            # Если нет nomenclature_id, добавляем пустые массивы
            for price in prices_db:
                price["photos"] = []

    count_query = (
        select(func.count(prices.c.id).label("count_prices"))
        .select_from(prices)
        .join(nomenclature, nomenclature.c.id == prices.c.nomenclature)
        .join(price_types, price_types.c.id == prices.c.price_type, isouter=True)
        .where(
            prices.c.cashbox == user.cashbox_id,
            prices.c.is_deleted == False,
            nomenclature.c.is_deleted == False,
            *filters_price,
            *filters_nom,
            *filters_price_type,
        )
    )

    prices_db_count = await database.fetch_one(count_query)

    return {"result": prices_db, "count": prices_db_count.count_prices}


@router.post("/prices/", response_model=schemas.PriceList)
async def new_price(token: str, prices_data: schemas.PriceCreateMass):
    """Создание цен"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    price_types_cache = set()
    nomenclature_cache = set()
    exceptions = []
    logging.info(f"Creating prices, count: {len(prices_data.dict()['__root__'])}")
    for price_values in prices_data.dict()["__root__"]:
        logging.info(
            f"Processing price: nomenclature={price_values.get('nomenclature')}, address={price_values.get('address')}"
        )
        price_values["owner"] = user.id
        price_values["is_deleted"] = False
        price_values["cashbox"] = user.cashbox_id

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

        # Нормализуем адрес (убираем пробелы в начале и конце)
        address = price_values.get("address")
        if address:
            address = address.strip()
            # Если после strip адрес стал пустым, удаляем его
            if address:
                price_values["address"] = address
            else:
                price_values["address"] = None
                address = None
        else:
            address = None

        # Проверка на дубликаты: запрещаем создание цены с одинаковым адресом для одной номенклатуры
        # Проверяем только если адрес не пустой
        if address and price_values.get("nomenclature"):
            logging.info(
                f"Checking for duplicate: nomenclature_id={price_values['nomenclature']}, address='{address}', owner={user.id}"
            )
            duplicate_query = prices.select().where(
                and_(
                    prices.c.owner == user.id,
                    prices.c.is_deleted.is_not(True),
                    prices.c.nomenclature == price_values["nomenclature"],
                    prices.c.address == address,
                )
            )
            existing_price = await database.fetch_one(duplicate_query)
            if existing_price:
                error_msg = (
                    f"Цена с адресом '{address}' уже существует для этого товара"
                )
                logging.warning(
                    f"Duplicate price detected: {error_msg}, nomenclature_id={price_values['nomenclature']}, address='{address}', existing_price_id={existing_price.id}"
                )
                exceptions.append(error_msg)
                continue
            else:
                logging.info(
                    f"No duplicate found for nomenclature_id={price_values['nomenclature']}, address='{address}'"
                )

        query = prices.insert().values(price_values)
        price_id = await database.execute(query)
        inserted_ids.add(price_id)

    # Если все цены были отклонены из-за ошибок, возвращаем ошибку сразу
    if not inserted_ids and exceptions:
        # Если только одна ошибка, возвращаем её напрямую, иначе объединяем
        error_message = (
            exceptions[0]
            if len(exceptions) == 1
            else "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )
        logging.error(
            f"Price creation failed: inserted_ids={inserted_ids}, exceptions_count={len(exceptions)}, error_message='{error_message}'"
        )
        raise HTTPException(
            status_code=400,
            detail=error_message,
        )

    query = prices.select().where(
        prices.c.cashbox == user.cashbox_id, prices.c.id.in_(inserted_ids)
    )
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
            nomenclature.c.cashbox == user.cashbox_id,
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
                    q = manufacturers.select().where(
                        manufacturers.c.id == nom_db.manufacturer
                    )
                    manufacturer = await database.fetch_one(q)

                    if manufacturer:
                        response_body["manufacturer"] = manufacturer.id
                        response_body["manufacturer_name"] = manufacturer.name

        response_body = datetime_to_timestamp(response_body)
        response_body_list.append(response_body)

    websocket_body = parse_obj_as(
        Optional[List[schemas.PriceInList]], response_body_list
    )
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
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

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
        dates_filters.append(
            prices.c.date_from <= date_from, prices.c.date_to <= date_to
        )

    q = prices.select().where(prices.c.id == idx, *dates_filters)
    price_db = await database.fetch_one(q)

    price_db = await get_entity_by_id(prices, price_db.id, user.cashbox_id)
    price_values = price.dict(exclude_unset=True)
    # Получаем все поля для проверки price_type (может не попасть в exclude_unset=True)
    price_dict_all = price.dict(exclude_unset=False)

    if price_values:
        # Обрабатываем price_type - проверяем из всех переданных полей
        # Если price_type не передан явно, сохраняем существующее значение
        if "price_type" not in price_dict_all:
            # price_type не передан в запросе - сохраняем существующее значение
            if price_db.price_type:
                price_values["price_type"] = price_db.price_type
        else:
            # price_type передан в запросе - обрабатываем его
            price_type_value = price_dict_all.get("price_type")
            if price_type_value is not None:
                # Если price_type пришел как строка, игнорируем его (фронтенд должен отправлять ID)
                if isinstance(price_type_value, str):
                    if "price_type" in price_values:
                        price_values.pop("price_type")
                    # Сохраняем существующее значение, если оно есть
                    if price_db.price_type:
                        price_values["price_type"] = price_db.price_type
                else:
                    # Проверяем существование типа цены
                    await get_entity_by_id(
                        price_types, price_type_value, user.cashbox_id
                    )
                    # Добавляем в price_values для обновления
                    price_values["price_type"] = price_type_value

        # Нормализуем адрес (убираем пробелы в начале и конце)
        if price_values.get("address"):
            price_values["address"] = price_values["address"].strip()

        # Обрабатываем адрес и координаты - если адрес есть, но координат нет - получаем их автоматически
        if price_values.get("address") and (
            not price_values.get("latitude") or not price_values.get("longitude")
        ):
            structured_geo = await geocoder.validate_address(price_values["address"])
            if structured_geo and structured_geo.latitude and structured_geo.longitude:
                price_values["latitude"] = structured_geo.latitude
                price_values["longitude"] = structured_geo.longitude

        # Проверка на дубликаты: запрещаем редактирование цены с одинаковым адресом для одной номенклатуры
        # Проверяем только если адрес не пустой
        address = price_values.get("address")
        if address and price_db.nomenclature:
            duplicate_query = prices.select().where(
                and_(
                    prices.c.owner == user.id,
                    prices.c.is_deleted.is_not(True),
                    prices.c.nomenclature == price_db.nomenclature,
                    prices.c.address == address,
                    prices.c.id != idx,  # Исключаем текущую цену
                )
            )
            existing_price = await database.fetch_one(duplicate_query)
            if existing_price:
                raise HTTPException(
                    status_code=400,
                    detail="Цена с таким адресом уже существует для этого товара",
                )

        query = (
            prices.update()
            .where(prices.c.id == idx, prices.c.cashbox == user.cashbox_id)
            .values(price_values)
        )
        await database.execute(query)
        price_db = await get_entity_by_id(prices, price_db.id, user.cashbox_id)

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
            nomenclature.c.cashbox == user.cashbox_id,
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
                    q = manufacturers.select().where(
                        manufacturers.c.id == nom_db.manufacturer
                    )
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
            dates_filters.append(
                prices.c.date_from <= date_from, prices.c.date_to <= date_to
            )

        q = prices.select().where(prices.c.id == price.id, *dates_filters)
        price_db = await database.fetch_one(q)

        price_values = price.dict(exclude_unset=True)

        if price_values:
            if price_values.get("price_type") is not None:
                await get_entity_by_id(
                    price_types, price_values["price_type"], user.cashbox_id
                )
            if price_values.get("nomenclature") is not None:
                await get_entity_by_id(
                    nomenclature, price_values["nomenclature"], user.cashbox_id
                )

            # if price_values.get("price_type") is not None and price_values.get("nomenclature") is not None:
            #     q = prices.select().where(prices.c.owner == user.id, prices.c.is_deleted == False, prices.c.nomenclature == price_values['nomenclature'], prices.c.price_type == price_values['price_type'])
            #     ex_price = await database.fetch_one(q)
            #     if ex_price:
            #         raise HTTPException(403, "Цена с таким типом уже существует")

            query = (
                prices.update()
                .where(prices.c.id == price_db.id, prices.c.cashbox == user.cashbox_id)
                .values(price_values)
            )
            await database.execute(query)
            price_db = await get_entity_by_id(prices, price_db.id, user.cashbox_id)

        response_body = {**dict(price_db)}

        response_body["id"] = price_db.id
        response_body["price"] = price_db.price
        response_body["date_to"] = price_db.date_to
        response_body["date_from"] = price_db.date_from
        response_body["updated_at"] = price_db.updated_at
        response_body["created_at"] = price_db.created_at

        q = nomenclature.select().where(
            nomenclature.c.id == price_db.nomenclature,
            nomenclature.c.cashbox == user.cashbox_id,
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
                    q = manufacturers.select().where(
                        manufacturers.c.id == nom_db.manufacturer
                    )
                    manufacturer = await database.fetch_one(q)

                    if manufacturer:
                        response_body["manufacturer"] = manufacturer.id
                        response_body["manufacturer_name"] = manufacturer.name

        response_body = datetime_to_timestamp(response_body)
        response_body_list.append(response_body)

    websocket_body = parse_obj_as(
        Optional[List[schemas.PriceInList]], response_body_list
    )
    websocket_body = [body.dict() for body in websocket_body]

    await manager.send_message(
        token,
        {"action": "edit", "target": "prices", "result": websocket_body},
    )

    return response_body_list


@router.delete("/prices/{idx}/", response_model=schemas.PriceInList)
async def delete_price(
    token: str, idx: int, date_from: Optional[int] = None, date_to: Optional[int] = None
):
    """Удаление цены"""
    user = await get_user_by_token(token)

    dates_filters = []
    if date_from and not date_to:
        dates_filters.append(prices.c.date_from <= date_from)
    if not date_from and date_to:
        dates_filters.append(prices.c.date_to <= date_to)
    if date_from and date_to:
        dates_filters.append(
            prices.c.date_from <= date_from, prices.c.date_to <= date_to
        )

    await get_entity_by_id(prices, idx, user.cashbox_id)

    query = prices.select().where(
        prices.c.id == idx,
        prices.c.cashbox == user.cashbox_id,
        prices.c.is_deleted == False,
    )
    price_db = await database.fetch_one(query)

    query = (
        prices.update()
        .where(prices.c.id == idx, prices.c.cashbox == user.cashbox_id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    response_body = {**dict(price_db)}

    response_body["id"] = price_db.id
    response_body["price"] = price_db.price
    response_body["date_to"] = price_db.date_to
    response_body["date_from"] = price_db.date_from
    response_body["updated_at"] = price_db.updated_at
    response_body["created_at"] = price_db.created_at

    q = nomenclature.select().where(
        nomenclature.c.id == price_db.nomenclature,
        nomenclature.c.cashbox == user.cashbox_id,
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
                q = manufacturers.select().where(
                    manufacturers.c.id == nom_db.manufacturer
                )
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
async def delete_price_mass(
    token: str, ids: str, date_from: Optional[int] = None, date_to: Optional[int] = None
):
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
            dates_filters.append(
                prices.c.date_from <= date_from, prices.c.date_to <= date_to
            )

        await get_entity_by_id(prices, int(price_id), user.cashbox_id)

        query = (
            prices.update()
            .where(prices.c.id == int(price_id), prices.c.cashbox == user.cashbox_id)
            .values({"is_deleted": True})
        )
        await database.execute(query)

        query = prices.select().where(
            prices.c.id == int(price_id), prices.c.cashbox == user.cashbox_id
        )
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
            nomenclature.c.cashbox == user.cashbox_id,
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
                    q = manufacturers.select().where(
                        manufacturers.c.id == nom_db.manufacturer
                    )
                    manufacturer = await database.fetch_one(q)

                    if manufacturer:
                        response_body["manufacturer"] = manufacturer.id
                        response_body["manufacturer_name"] = manufacturer.name

        response_body = datetime_to_timestamp(response_body)
        response_body_list.append(response_body)

    websocket_body = parse_obj_as(
        Optional[List[schemas.PriceInList]], response_body_list
    )
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

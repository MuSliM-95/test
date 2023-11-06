from api.pagination.pagination import Page
from database.db import (
    database,
    docs_warehouse,
    docs_warehouse_goods,
    organizations,
    price_types,
    warehouse_balances,
    warehouse_register_movement,
    warehouses,
    OperationType
)
from . import schemas
from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi_pagination import add_pagination, paginate
from api.pagination.pagination import Page
from functions.helpers import (
    check_entity_exists,
    check_period_blocked,
    check_unit_exists,
    datetime_to_timestamp,
    get_user_by_token,
)
from sqlalchemy import desc, asc, select, func
from ws_manager import manager

from typing import List

from api.docs_warehouses.func_warehouse import call_type_movement, set_data_doc_warehouse, update_docs_warehouse, update_goods_warehouse

router = APIRouter(tags=["docs_warehouse"])

contragents_cache = set()
organizations_cache = set()
contracts_cache = set()
warehouses_cache = set()
users_cache = set()
price_types_cache = set()
units_cache = set()

Page = Page.with_custom_options(
    size=Query(10, ge=1, le=100),
)

@router.get("/docs_warehouse/{idx}/", response_model=schemas.View)
async def get_by_id(token: str, idx: int):
    """Получение документа по ID"""
    await get_user_by_token(token)
    query = docs_warehouse.select().where(docs_warehouse.c.id == idx, docs_warehouse.c.is_deleted.is_not(True))
    instance_db = await database.fetch_one(query)

    if not instance_db:
        raise HTTPException(status_code=404, detail=f"Не найдено.")

    instance_db = datetime_to_timestamp(instance_db)

    query = docs_warehouse_goods.select().where(docs_warehouse_goods.c.docs_warehouse_id == idx)
    goods_db = await database.fetch_all(query)
    goods_db = [*map(datetime_to_timestamp, goods_db)]
    instance_db["goods"] = goods_db

    return instance_db


@router.get("/docs_warehouse/", response_model=schemas.GetDocsWarehouse)
async def get_list(token: str, limit: int = 10, offset: int = 0, datefrom: int = None, dateto: int = None, tags: str = None):
    """Получение списка документов"""
    filters_list = []
    user = await get_user_by_token(token)

    if datefrom and not dateto:
        filters_list.append(docs_warehouse.c.dated >= datefrom)
    if not datefrom and dateto:
        filters_list.append(docs_warehouse.c.dated <= dateto)
    if datefrom and dateto:
        filters_list.append(docs_warehouse.c.dated >= datefrom)
        filters_list.append(docs_warehouse.c.dated <= dateto)
    
    if tags:
        filters_list.append(docs_warehouse.c.tags.ilike(f"%{tags}%"))

    query = docs_warehouse.select().where(docs_warehouse.c.is_deleted.is_not(True), docs_warehouse.c.cashbox == user.cashbox_id).order_by(desc(docs_warehouse.c.id)).where(*filters_list).limit(limit).offset(offset)
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    query = select(func.count(docs_warehouse.c.id)).where(docs_warehouse.c.is_deleted.is_not(True), docs_warehouse.c.cashbox == user.cashbox_id).where(*filters_list)
    count = await database.fetch_one(query)

    return {"result": items_db, "count": count.count_1}


async def check_foreign_keys(instance_values, user, exceptions) -> bool:
    if instance_values.get("organization") is not None:
        if instance_values["organization"] not in organizations_cache:
            try:
                await check_entity_exists(organizations, instance_values["organization"], user.id)
                organizations_cache.add(instance_values["organization"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("warehouse") is not None:
        if instance_values["warehouse"] not in warehouses_cache:
            try:
                await check_entity_exists(warehouses, instance_values["warehouse"], user.id)
                warehouses_cache.add(instance_values["warehouse"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False
    return True


@router.post("/docs_warehouse/", response_model=schemas.ListView)
async def create(token: str, docs_warehouse_data: schemas.CreateMass):
    """Создание документов"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    exceptions = []
    for instance_values in docs_warehouse_data.dict()["__root__"]:
        instance_values["created_by"] = user.id
        instance_values["cashbox"] = user.cashbox_id

        if not await check_period_blocked(instance_values["organization"], instance_values.get("dated"), exceptions):
            continue

        if not await check_foreign_keys(
            instance_values,
            user,
            exceptions,
        ):
            continue

        goods: list = instance_values.get("goods")
        try:
            del instance_values["goods"]
        except KeyError:
            pass
        query = docs_warehouse.insert().values(instance_values)
        instance_id = await database.execute(query)
        inserted_ids.add(instance_id)
        items_sum = 0
        for item in goods:
            item["docs_warehouse_id"] = instance_id

            if item.get("price_type") is not None:
                if item["price_type"] not in price_types_cache:
                    try:
                        await check_entity_exists(price_types, item["price_type"], user.id)
                        price_types_cache.add(item["price_type"])
                    except HTTPException as e:
                        exceptions.append(str(item) + " " + e.detail)
                        continue
            if item.get("unit") is not None:
                if item["unit"] not in units_cache:
                    try:
                        await check_unit_exists(item["unit"])
                        units_cache.add(item["unit"])
                    except HTTPException as e:
                        exceptions.append(str(item) + " " + e.detail)
                        continue
            query = docs_warehouse_goods.insert().values(item)
            await database.execute(query)
            items_sum += item["price"] * item["quantity"]
            if instance_values.get("warehouse") is not None and not instance_values.get("docs_sales_id"):
                query = (
                    warehouse_balances.select()
                    .where(
                        warehouse_balances.c.warehouse_id == instance_values["warehouse"],
                        warehouse_balances.c.nomenclature_id == item["nomenclature"],
                    )
                    .order_by(desc(warehouse_balances.c.created_at))
                )
                last_warehouse_balance = await database.fetch_one(query)
                warehouse_amount = last_warehouse_balance.current_amount if last_warehouse_balance else 0
                warehouse_amount_incoming = (
                    last_warehouse_balance.incoming_amount
                    if last_warehouse_balance and last_warehouse_balance.incoming_amount
                    else 0
                )
                warehouse_amount_outgoing = (
                    last_warehouse_balance.outgoing_amount
                    if last_warehouse_balance and last_warehouse_balance.outgoing_amount
                    else 0
                )

                query = warehouse_balances.delete().where(
                    warehouse_balances.c.warehouse_id == instance_values["warehouse"],
                    warehouse_balances.c.nomenclature_id == item["nomenclature"],
                    warehouse_balances.c.organization_id == instance_values["organization"],
                )
                await database.execute(query)

                query = warehouse_balances.insert().values(
                    {
                        "organization_id": instance_values["organization"],
                        "warehouse_id": instance_values["warehouse"],
                        "nomenclature_id": item["nomenclature"],
                        "document_warehouse_id": instance_id,
                        "incoming_amount": warehouse_amount_incoming + item["quantity"],
                        "outgoing_amount": warehouse_amount_outgoing,
                        "current_amount": warehouse_amount + item["quantity"],
                        "cashbox_id": user.id,
                    }
                )
                await database.execute(query)
        query = docs_warehouse.update().where(docs_warehouse.c.id == instance_id).values({"sum": items_sum})
        await database.execute(query)

    query = docs_warehouse.select().where(docs_warehouse.c.id.in_(inserted_ids))
    docs_warehouse_db = await database.fetch_all(query)
    docs_warehouse_db = [*map(datetime_to_timestamp, docs_warehouse_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "docs_warehouse",
            "result": docs_warehouse_db,
        },
    )

    if exceptions:
        raise HTTPException(400, "Не были добавлены следующие записи: " + ", ".join(exceptions))

    return docs_warehouse_db


@router.patch("/docs_warehouse/", response_model=schemas.ListView)
async def update(token: str, docs_warehouse_data: schemas.EditMass):
    """Редактирование документов"""
    user = await get_user_by_token(token)

    updated_ids = set()
    exceptions = []
    for instance_values in docs_warehouse_data.dict(exclude_unset=True)["__root__"]:
        if not await check_period_blocked(instance_values["organization"], instance_values.get("dated"), exceptions):
            continue

        if not await check_foreign_keys(instance_values, user, exceptions):
            continue

        goods: list = instance_values.get("goods")
        try:
            del instance_values["goods"]
        except KeyError:
            pass
        query = docs_warehouse.update().where(docs_warehouse.c.id == instance_values["id"]).values(instance_values)
        await database.execute(query)
        instance_id = instance_values["id"]
        updated_ids.add(instance_id)
        if goods:
            query = docs_warehouse_goods.delete().where(docs_warehouse_goods.c.docs_warehouse_id == instance_id)
            await database.execute(query)
            items_sum = 0
            for item in goods:
                item["docs_warehouse_id"] = instance_id

                if item.get("price_type") is not None:
                    if item["price_type"] not in price_types_cache:
                        try:
                            await check_entity_exists(price_types, item["price_type"], user.id)
                            price_types_cache.add(item["price_type"])
                        except HTTPException as e:
                            exceptions.append(str(item) + " " + e.detail)
                            continue
                if item.get("unit") is not None:
                    if item["unit"] not in units_cache:
                        try:
                            await check_unit_exists(item["unit"])
                            units_cache.add(item["unit"])
                        except HTTPException as e:
                            exceptions.append(str(item) + " " + e.detail)
                            continue
                query = docs_warehouse_goods.insert().values(item)
                await database.execute(query)
                items_sum += item["price"] * item["quantity"]
                if instance_values.get("warehouse") is not None:
                    query = (
                        warehouse_balances.select()
                        .where(
                            warehouse_balances.c.warehouse_id == instance_values["warehouse"],
                            warehouse_balances.c.nomenclature_id == item["nomenclature"],
                            warehouse_balances.c.organization_id == instance_values["organization"],
                        )
                        .order_by(desc(warehouse_balances.c.created_at))
                    )
                    last_warehouse_balance = await database.fetch_one(query)
                    warehouse_amount = last_warehouse_balance.current_amount if last_warehouse_balance else 0
                    warehouse_amount_incoming = (
                        last_warehouse_balance.incoming_amount
                        if last_warehouse_balance and last_warehouse_balance.incoming_amount
                        else 0
                    )
                    warehouse_amount_outgoing = (
                        last_warehouse_balance.outgoing_amount
                        if last_warehouse_balance and last_warehouse_balance.outgoing_amount
                        else 0
                    )

                    query = warehouse_balances.delete().where(
                        warehouse_balances.c.warehouse_id == instance_values["warehouse"],
                        warehouse_balances.c.nomenclature_id == item["nomenclature"],
                        warehouse_balances.c.organization_id == instance_values["organization"],
                    )
                    await database.execute(query)

                    query = warehouse_balances.insert().values(
                        {
                            "organization_id": instance_values["organization"],
                            "warehouse_id": instance_values["warehouse"],
                            "nomenclature_id": item["nomenclature"],
                            "document_warehouse_id": instance_id,
                            "incoming_amount": warehouse_amount_incoming + item["quantity"],
                            "outgoing_amount": warehouse_amount_outgoing,
                            "current_amount": warehouse_amount + item["quantity"],
                            "cashbox_id": user.id,
                        }
                    )
                    await database.execute(query)

            query = docs_warehouse.update().where(docs_warehouse.c.id == instance_id).values({"sum": items_sum})
            await database.execute(query)

    query = docs_warehouse.select().where(docs_warehouse.c.id.in_(updated_ids))
    docs_warehouse_db = await database.fetch_all(query)
    docs_warehouse_db = [*map(datetime_to_timestamp, docs_warehouse_db)]

    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "docs_warehouse",
            "result": docs_warehouse_db,
        },
    )

    if exceptions:
        raise HTTPException(400, "Не были добавлены следующие записи: " + ", ".join(exceptions))

    return docs_warehouse_db


@router.delete("/docs_warehouse/{idx}/")
@database.transaction()
async def delete(token: str, ids: list[int]):
    """Удаление документов"""
    await get_user_by_token(token)

    query = docs_warehouse.select().where(docs_warehouse.c.id.in_(ids), docs_warehouse.c.is_deleted.is_not(True))
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            docs_warehouse.update()
            .where(docs_warehouse.c.id.in_(ids), docs_warehouse.c.is_deleted.is_not(True))
            .values({"is_deleted": True})
        )
        await database.execute(query)

        """ Изменение остатка на складе - удаление движения в регистре """
        try:
            for item in items_db:
                query = warehouse_register_movement.select()\
                        .where(
                    warehouse_register_movement.c.document_warehouse_id == item['id']
                )
                result = await database.fetch_all(query)
                item.update({'deleted': result})
                query = warehouse_register_movement.delete()\
                        .where(
                    warehouse_register_movement.c.document_warehouse_id == item['id']
                )
                await database.execute(query)
        except Exception as error:
            raise HTTPException(status_code=433, detail=str(error))

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_warehouse",
                "result": items_db,
            },
        )

    return items_db


@router.post("/alt_docs_warehouse/",
             tags=["Alternative docs_warehouse"], response_model=schemas.ListView)
async def create(
        token: str,
        docs_warehouse_data: schemas.CreateMass):
    """
    Создание документов движения товарных остатков
    operation:
        incoming Приходных (Увеличивает количество товара на складе)
        outgoing Расходных (Уменьшает количество товара на складе)
        transfer Переводных документов (Уменьшает на одном складе увеличивает на другом)
    """
    response: list = []
    docs_warehouse_data = docs_warehouse_data.dict()
    user = await get_user_by_token(token)
    for doc in docs_warehouse_data["__root__"]:
        response.append(await call_type_movement(doc['operation'], entity_values=doc, token=token))
    query = docs_warehouse.select().where(docs_warehouse.c.id.in_(response))
    docs_warehouse_db = await database.fetch_all(query)
    docs_warehouse_db = [*map(datetime_to_timestamp, docs_warehouse_db)]

    q = docs_warehouse.select().where(
        docs_warehouse.c.cashbox == user.cashbox_id,
        docs_warehouse.c.is_deleted == False
    ).order_by(asc(docs_warehouse.c.id))

    docs_db = await database.fetch_all(q)

    for i in range(0, len(docs_db)):
        if not docs_db[i].number:
            q = docs_warehouse.update().where(docs_warehouse.c.id == docs_db[i].id).values({ "number": str(i + 1) })
            await database.execute(q)

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "docs_warehouse",
            "result": docs_warehouse_db,
        },
    )

    return docs_warehouse_db


@database.transaction()
@router.patch("/alt_docs_warehouse/",
              tags=["Alternative docs_warehouse"],
              response_model=schemas.ListView)
async def update(token: str, docs_warehouse_data: schemas.EditMass):
    """
    Обновление
    """
    response: list = []
    docs_warehouse_data = docs_warehouse_data.dict(exclude_unset=True)

    for doc in docs_warehouse_data["__root__"]:

        if doc.get('goods'):
            goods: list = doc['goods']
            del doc['goods']

        else:
            goods = await database.fetch_all(docs_warehouse_goods.select().where(docs_warehouse_goods.c.docs_warehouse_id == doc['id']))

        stored_item_data = await database.fetch_one(
            docs_warehouse.select().where(docs_warehouse.c.id == doc['id']))
        stored_item_model = schemas.Edit(**stored_item_data)
        updated_item = stored_item_model.copy(update=doc)
        doc = jsonable_encoder(updated_item)
        del doc['goods']

        entity = await set_data_doc_warehouse(entity_values=doc, token=token)
        doc_id = await update_docs_warehouse(entity=entity)
        entity.update({'goods': goods})
        if entity['operation'] == "incoming":
            await update_goods_warehouse(entity=entity, doc_id=doc_id, type_operation=OperationType.plus)
            response.append(doc_id)
        if entity['operation'] == "outgoing":
            await update_goods_warehouse(entity=entity, doc_id=doc_id, type_operation=OperationType.minus)
            response.append(doc_id)
        if entity['operation'] == "transfer":
            await update_goods_warehouse(entity=entity, doc_id=doc_id, type_operation=OperationType.minus)
            entity.update({'warehouse': entity['to_warehouse']})
            await update_goods_warehouse(entity=entity, doc_id=doc_id, type_operation=OperationType.plus)
        response.append(doc_id)

    query = docs_warehouse.select().where(docs_warehouse.c.id.in_(response))
    docs_warehouse_db = await database.fetch_all(query)
    docs_warehouse_db = [*map(datetime_to_timestamp, docs_warehouse_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "docs_warehouse",
            "result": docs_warehouse_db,
        },
    )

    return docs_warehouse_db


add_pagination(router)


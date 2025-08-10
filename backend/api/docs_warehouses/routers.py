from api.pagination.pagination import Page
from database.db import (
    database,
    docs_warehouse,
    docs_warehouse_goods,
    organizations,
    nomenclature,
    price_types,
    warehouse_balances,
    warehouse_register_movement,
    warehouses,
    units,
    OperationType,
    docs_warehouse_goods,
    cashbox_settings
)
from . import schemas
from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi_pagination import add_pagination, paginate
from api.pagination.pagination import Page
from functions.helpers import (
    check_entity_exists ,
    check_period_blocked ,
    check_unit_exists ,
    datetime_to_timestamp ,
    get_user_by_token , add_nomenclature_name_to_goods ,
)
from sqlalchemy import desc, asc, select, func
from ws_manager import manager

from typing import List

from api.docs_warehouses.func_warehouse import call_type_movement, set_data_doc_warehouse, update_docs_warehouse, update_goods_warehouse, validate_photo_for_writeoff
from api.docs_warehouses.schemas import WarehouseOperations

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
    # instance_db["goods"] = goods_db

    goods = []
    for good in goods_db:
        nomenclature_db = await database.fetch_one(nomenclature.select().where(nomenclature.c.id == good["nomenclature"]))
        unit_db = await database.fetch_one(units.select().where(units.c.id == good['unit']))
        goods.append({
            "price_type": good['price_type'],
            "price": good['price'],
            "quantity": good['quantity'],
            "unit": good['unit'],
            "nomenclature": good['nomenclature'],
            "unit_name": unit_db.name,
            "nomenclature_name": nomenclature_db.name
        })

    instance_db["goods"] = goods

    return instance_db


@router.get("/docs_warehouse/", response_model=schemas.GetDocsWarehouse)
async def get_list(token: str, warehouse_id: int = None, operation: str = '', show_goods: bool = False, limit: int = 10, offset: int = 0, datefrom: int = None, dateto: int = None, tags: str = None):
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

    if operation:
        filters_list.append(docs_warehouse.c.operation == operation)

    if warehouse_id:
        filters_list.append(docs_warehouse.c.warehouse == warehouse_id)

    query = docs_warehouse.select().where(docs_warehouse.c.is_deleted.is_not(True), docs_warehouse.c.cashbox == user.cashbox_id).order_by(desc(docs_warehouse.c.id)).where(*filters_list).limit(limit).offset(offset)
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if show_goods:
        for item in items_db:
            query = docs_warehouse_goods.select().where(docs_warehouse_goods.c.docs_warehouse_id == item['id'])
            goods_db = await database.fetch_all(query)
            print(goods_db)
            goods_db = [*map(datetime_to_timestamp, goods_db)]

            goods_db = [*map(add_nomenclature_name_to_goods, goods_db)]
            goods_db = [await instance for instance in goods_db]
            print(goods_db)
            item['goods'] = goods_db

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

        if instance_values["operation"] == "write_off":
            instance_values["status"] = False

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
            else:
                q = nomenclature.select().where(nomenclature.c.id == item['nomenclature'])
                nom_db = await database.fetch_one(q)
                item['unit'] = nom_db.unit

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

        query = docs_warehouse.select().where(docs_warehouse.c.id == instance_values["id"])
        instance_db = await database.fetch_one(query)

        if instance_values.get("status") is True and instance_db.operation == "write_off":
            try:
                await validate_photo_for_writeoff(instance_values["id"], user.id)
            except HTTPException as e:
                exceptions.append(f"Документ {instance_values['id']}: {e.detail}")
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
                print(item)
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
                else:
                    q = nomenclature.select().where(nomenclature.c.id == item['nomenclature'])
                    nom_db = await database.fetch_one(q)
                    item['unit'] = nom_db.unit

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


@database.transaction()
@router.delete("/docs_warehouse/")
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
        docs_warehouse_data: schemas.CreateMass,
        holding: bool = False
):
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

        for doc_good in doc['goods']:
            if not doc_good['unit']:
                q = nomenclature.select().where(nomenclature.c.id == doc_good['nomenclature'])
                nom_db = await database.fetch_one(q)
                if nom_db:
                    if nom_db.unit:
                        doc_good['unit'] = nom_db.unit
                    else:
                        doc_good['unit'] = 116

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

    if holding:
        await update(token, schemas.EditMass(__root__=[{"id": doc["id"], "status": True} for doc in docs_warehouse_db]))
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


@database.transaction()
@router.delete("/docs_warehouse/{idx}")
async def delete_docs_warehouse_route(token: str, idx: int):
    """Удаление документа"""
    await get_user_by_token(token)

    query = docs_warehouse.select().where(docs_warehouse.c.id == idx, docs_warehouse.c.is_deleted.is_not(True))
    item_db = await database.fetch_one(query)
    item_db = datetime_to_timestamp(item_db)

    if item_db:
        query = (
            docs_warehouse.update()
            .where(docs_warehouse.c.id == idx, docs_warehouse.c.is_deleted.is_not(True))
            .values({"is_deleted": True})
        )
        await database.execute(query)

        """ Изменение остатка на складе - удаление движения в регистре """
        try:
            query = (
                warehouse_register_movement
                .select()
                .where(
                    warehouse_register_movement.c.document_warehouse_id == item_db['id']
                )
            )
            result = await database.fetch_all(query)
            item_db.update({'deleted': result})
            query = (
                warehouse_register_movement
                .delete()
                .where(
                    warehouse_register_movement.c.document_warehouse_id == item_db['id']
                )
            )
            await database.execute(query)
        except Exception as error:
            raise HTTPException(status_code=433, detail=str(error))

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_warehouse",
                "result": item_db,
            },
        )

    return item_db


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
        if entity['operation'] == "write_off":
            if doc.get("status") is True:
                # Получаем настройки кассы
                cashbox_settings_data = await database.fetch_one(
                    cashbox_settings.select().where(cashbox_settings.c.cashbox_id == entity["cashbox"])
                )
                require_photo = cashbox_settings_data and cashbox_settings_data["require_photo_for_writeoff"]

                # Если в настройках включено требование фото — проверяем
                if require_photo:
                    try:
                        await validate_photo_for_writeoff(doc["id"])
                    except HTTPException as e:
                        continue

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
        if entity['operation'] == "write_off":
            await update_goods_warehouse(entity=entity, doc_id=doc_id, type_operation=OperationType.minus)
            response.append(doc_id)



    query = docs_warehouse.select().where(docs_warehouse.c.id.in_(response))
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

    return docs_warehouse_db


add_pagination(router)


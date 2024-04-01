from database.db import (
    contracts,
    database,
    docs_purchases,
    docs_purchases_goods,
    nomenclature,
    organizations,
    price_types,
    users_cboxes_relation,
    warehouse_balances,
    warehouses,
    units,
    docs_warehouse
)
from fastapi import APIRouter, HTTPException
from fastapi_pagination import Page, paginate
from functions.helpers import (
    check_contragent_exists,
    check_entity_exists,
    check_period_blocked,
    check_unit_exists,
    datetime_to_timestamp,
    get_user_by_token,
)
from sqlalchemy import desc, func, select, asc
from ws_manager import manager

from . import schemas

from api.docs_warehouses.utils import create_warehouse_docs
from api.docs_warehouses.routers import update as update_warehouse_doc

router = APIRouter(tags=["docs_purchases"])

contragents_cache = set()
organizations_cache = set()
contracts_cache = set()
warehouses_cache = set()
users_cache = set()
price_types_cache = set()
units_cache = set()
nomenclature_cache = set()


@router.get("/docs_purchases/{idx}/", response_model=schemas.View)
async def get_by_id(token: str, idx: int):
    """Получение документа по ID"""
    await get_user_by_token(token)
    query = docs_purchases.select().where(docs_purchases.c.id == idx, docs_purchases.c.is_deleted.is_not(True))
    instance_db = await database.fetch_one(query)

    if not instance_db:
        raise HTTPException(status_code=404, detail=f"Не найдено.")

    instance_db = datetime_to_timestamp(instance_db)

    query = docs_purchases_goods.select().where(docs_purchases_goods.c.docs_purchases_id == idx)
    goods_db = await database.fetch_all(query)
    goods_db = [*map(datetime_to_timestamp, goods_db)]
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


@router.get("/docs_purchases/", response_model=schemas.ViewResult)
async def get_list(token: str, limit: int = 100, offset: int = 0, tags: str = None):
    """Получение списка документов"""
    user = await get_user_by_token(token)
    filters_list = []
    if tags:
        filters_list.append(docs_warehouse.c.tags.ilike(f"%{tags}%"))

    query = docs_purchases.select().where(docs_purchases.c.is_deleted.is_not(True), docs_purchases.c.cashbox == user.cashbox_id).where(*filters_list).order_by(desc(docs_purchases.c.id)).limit(limit).offset(offset)
    
    query_count = select(func.count(docs_purchases.c.id)).where(docs_purchases.c.is_deleted.is_not(True), docs_purchases.c.cashbox == user.cashbox_id).where(*filters_list)
    count = await database.fetch_one(query_count)

    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]
    return {"result": items_db, "count": count.count_1}


async def check_foreign_keys(instance_values, user, exceptions) -> bool:
    if instance_values.get("nomenclature") is not None:
        if instance_values["nomenclature"] not in nomenclature_cache:
            try:
                await check_entity_exists(nomenclature, instance_values["nomenclature"], user.id)
                nomenclature_cache.add(instance_values["nomenclature"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("client") is not None:
        if instance_values["client"] not in contragents_cache:
            try:
                await check_contragent_exists(instance_values["client"], user.cashbox_id)
                contragents_cache.add(instance_values["client"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("contragent") is not None:
        if instance_values["contragent"] not in contragents_cache:
            try:
                await check_contragent_exists(instance_values["contragent"], user.cashbox_id)
                contragents_cache.add(instance_values["contragent"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("contract") is not None:
        if instance_values["contract"] not in contracts_cache:
            try:
                await check_entity_exists(contracts, instance_values["contract"], user.id)
                contracts_cache.add(instance_values["contract"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

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

    if instance_values.get("purchased_by") is not None:
        if instance_values["purchased_by"] not in users_cache:
            query = users_cboxes_relation.select().where(users_cboxes_relation.c.id == instance_values["purchased_by"])
            if not await database.fetch_one(query):
                exceptions.append(str(instance_values) + " Пользователь не существует!")
                return False
            users_cache.add(instance_values["purchased_by"])
    return True


@router.post("/docs_purchases/", response_model=schemas.ListView)
async def create(token: str, docs_purchases_data: schemas.CreateMass):
    """Создание документов"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    exceptions = []
    for instance_values in docs_purchases_data.dict()["__root__"]:
        instance_values["created_by"] = user.id
        instance_values["cashbox"] = user.cashbox_id
        instance_values["is_deleted"] = False
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
        query = docs_purchases.insert().values(instance_values)
        instance_id = await database.execute(query)
        inserted_ids.add(instance_id)
        items_sum = 0
        for item in goods:
            item["docs_purchases_id"] = instance_id

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
            query = docs_purchases_goods.insert().values(item)
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
                        "document_purchase_id": instance_id,
                        "incoming_amount": warehouse_amount_incoming + item["quantity"],
                        "outgoing_amount": warehouse_amount_outgoing,
                        "current_amount": warehouse_amount + item["quantity"],
                        "cashbox_id": user.cashbox_id,
                    }
                )
                await database.execute(query)
        query = docs_purchases.update().where(docs_purchases.c.id == instance_id).values({"sum": items_sum})
        await database.execute(query)

        goods_res = []
        for good in goods:
            nomenclature_db = await database.fetch_one(nomenclature.select().where(nomenclature.c.id == good['nomenclature']))
            if nomenclature_db.type == "product":
                goods_res.append(
                    {
                        "price_type": 1,
                        "price": 0,
                        "quantity": good['quantity'],
                        "unit": good['unit'],
                        "nomenclature": good['nomenclature']
                    }
                )


        body = {
            "number": None,
            "dated": instance_values['dated'],
            "docs_purchases": None,
            "to_warehouse": None,
            "organization":instance_values['organization'],
            "status": False,
            "contragent": instance_values['contragent'],
            "operation": "incoming",
            "comment": instance_values['comment'],
            "warehouse": instance_values['warehouse'],
            "docs_sales_id": instance_id,
            "goods": goods_res
        }

        body['docs_purchases'] = None
        body['number'] = None
        body['to_warehouse'] = None
        await create_warehouse_docs(token, body, user.cashbox_id)

    q = docs_purchases.select().where(
        docs_purchases.c.cashbox == user.cashbox_id,
        docs_purchases.c.is_deleted == False
    ).order_by(asc(docs_purchases.c.id))

    docs_db = await database.fetch_all(q)

    for i in range(0, len(docs_db)):
        if not docs_db[i].number:
            q = docs_purchases.update().where(docs_purchases.c.id == docs_db[i].id).values({ "number": str(i + 1) })
            await database.execute(q)


    query = docs_purchases.select().where(docs_purchases.c.id.in_(inserted_ids))
    docs_purchases_db = await database.fetch_all(query)
    docs_purchases_db = [*map(datetime_to_timestamp, docs_purchases_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "docs_purchases",
            "result": docs_purchases_db,
        },
    )

    if exceptions:
        raise HTTPException(400, "Не были добавлены следующие записи: " + ", ".join(exceptions))

    return docs_purchases_db


@router.patch("/docs_purchases/", response_model=schemas.ListView)
async def update(token: str, docs_purchases_data: schemas.EditMass):
    """Редактирование документов"""
    user = await get_user_by_token(token)

    updated_ids = set()
    exceptions = []
    for instance_values in docs_purchases_data.dict(exclude_unset=True)["__root__"]:
        if not await check_period_blocked(instance_values["organization"], instance_values.get("dated"), exceptions):
            continue
        if not await check_foreign_keys(instance_values, user, exceptions):
            continue

        goods: list = instance_values.get("goods")
        try:
            del instance_values["goods"]
        except KeyError:
            pass

        query = docs_purchases.update().where(docs_purchases.c.id == instance_values["id"]).values(instance_values)
        await database.execute(query)
        instance_id = instance_values["id"]
        updated_ids.add(instance_id)
        if goods:
            query = docs_purchases_goods.delete().where(docs_purchases_goods.c.docs_purchases_id == instance_id)
            await database.execute(query)
            items_sum = 0
            for item in goods:
                item["docs_purchases_id"] = instance_id

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
                query = docs_purchases_goods.insert().values(item)
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

                    query = warehouse_balances.delete().where(
                        warehouse_balances.c.warehouse_id == instance_values["warehouse"],
                        warehouse_balances.c.nomenclature_id == item["nomenclature"],
                        warehouse_balances.c.organization_id == instance_values["organization"],
                    )
                    await database.execute(query)

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

                    query = warehouse_balances.insert().values(
                        {
                            "organization_id": instance_values["organization"],
                            "warehouse_id": instance_values["warehouse"],
                            "nomenclature_id": item["nomenclature"],
                            "document_purchase_id": instance_id,
                            "incoming_amount": warehouse_amount_incoming + item["quantity"],
                            "outgoing_amount": warehouse_amount_outgoing,
                            "current_amount": warehouse_amount + item["quantity"],
                            "cashbox_id": user.cashbox_id,
                        }
                    )
                    await database.execute(query)

            query = docs_purchases.update().where(docs_purchases.c.id == instance_id).values({"sum": items_sum})
            await database.execute(query)

            # doc_warehouse = await database.fetch_one(docs_warehouse.select().where(docs_warehouse.c.docs_sales_id == instance_id).order_by(desc(docs_warehouse.c.id)))

            # goods_res = []
            # for good in goods:
            #     nomenclature_db = await database.fetch_one(nomenclature.select().where(nomenclature.c.id == good['nomenclature']))
            #     if nomenclature_db.type == "product":
            #         goods_res.append(
            #             {
            #                 "price_type": good['price_type'],
            #                 "price": good['price'],
            #                 "quantity": good['quantity'],
            #                 "unit": good['unit'],
            #                 "nomenclature": good['nomenclature']
            #             }
            #         )


            # body = [{
            #     "id": doc_warehouse.id,
            #     "dated": instance_values['dated'],
            #     "contragent": instance_values['contragent'],
            #     "operation": "incoming",
            #     "comment": instance_values['comment'],
            #     "warehouse": instance_values['warehouse'],
            #     "docs_sales_id": instance_id,
            #     "goods": goods_res
            # }]

            # await update_warehouse_doc(token, body)


    query = docs_purchases.select().where(docs_purchases.c.id.in_(updated_ids))
    docs_purchases_db = await database.fetch_all(query)
    docs_purchases_db = [*map(datetime_to_timestamp, docs_purchases_db)]

    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "docs_purchases",
            "result": docs_purchases_db,
        },
    )

    if exceptions:
        raise HTTPException(400, "Не были добавлены следующие записи: " + ", ".join(exceptions))

    return docs_purchases_db


@router.delete("/docs_purchases/", response_model=schemas.ListView)
async def delete(token: str, ids: list[int]):
    """Удаление документов"""
    user = await get_user_by_token(token)

    query = docs_purchases.select().where(docs_purchases.c.id.in_(ids), docs_purchases.c.is_deleted.is_not(True), docs_purchases.c.cashbox == user.cashbox_id)
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            docs_purchases.update()
            .where(docs_purchases.c.id.in_(ids), docs_purchases.c.is_deleted.is_not(True), docs_purchases.c.cashbox == user.cashbox_id)
            .values({"is_deleted": True})
        )
        await database.execute(query)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_purchases",
                "result": items_db,
            },
        )

    return items_db

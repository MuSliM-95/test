from fastapi import APIRouter, HTTPException
from sqlalchemy import desc

from database.db import (
    database,
    docs_purchases,
    organizations,
    docs_purchases_goods,
    contracts,
    warehouses,
    users_cboxes_relation,
    price_types,
    nomenclature,
    warehouse_balances,
)

from . import schemas

from functions.helpers import (
    datetime_to_timestamp,
    check_contragent_exists,
    check_entity_exists,
    check_unit_exists,
    check_period_blocked,
)
from functions.helpers import get_user_by_token

from ws_manager import manager

router = APIRouter(tags=["docs_purchases"])

contragents_cache = set()
organizations_cache = set()
contracts_cache = set()
warehouses_cache = set()
users_cache = set()
price_types_cache = set()
units_cache = set()
nomenclature_cache = set()


@router.get("/docs_purchases/{idx}", response_model=schemas.View)
async def get_by_id(token: str, idx: int):
    """Получение документа по ID"""
    await get_user_by_token(token)
    query = docs_purchases.select().where(
        docs_purchases.c.id == idx, docs_purchases.c.is_deleted.is_not(True)
    )
    instance_db = await database.fetch_one(query)

    if not instance_db:
        raise HTTPException(status_code=404, detail=f"Не найдено.")

    instance_db = datetime_to_timestamp(instance_db)

    query = docs_purchases_goods.select().where(
        docs_purchases_goods.c.docs_purchases_id == idx
    )
    goods_db = await database.fetch_all(query)
    goods_db = [*map(datetime_to_timestamp, goods_db)]
    instance_db["goods"] = goods_db

    return instance_db


@router.get("/docs_purchases/", response_model=schemas.ListView)
async def get_list(token: str, limit: int = 100, offset: int = 0):
    """Получение списка документов"""
    await get_user_by_token(token)
    query = (
        docs_purchases.select()
        .where(docs_purchases.c.is_deleted.is_not(True))
        .limit(limit)
        .offset(offset)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]
    return items_db


async def check_foreign_keys(instance_values, user, exceptions) -> bool:
    if instance_values.get("nomenclature") is not None:
        if instance_values["nomenclature"] not in nomenclature_cache:
            try:
                await check_entity_exists(
                    nomenclature, instance_values["nomenclature"], user.id
                )
                nomenclature_cache.add(instance_values["nomenclature"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("client") is not None:
        if instance_values["client"] not in contragents_cache:
            try:
                await check_contragent_exists(
                    instance_values["client"], user.cashbox_id
                )
                contragents_cache.add(instance_values["client"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("contragent") is not None:
        if instance_values["contragent"] not in contragents_cache:
            try:
                await check_contragent_exists(
                    instance_values["contragent"], user.cashbox_id
                )
                contragents_cache.add(instance_values["contragent"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("contract") is not None:
        if instance_values["contract"] not in contracts_cache:
            try:
                await check_entity_exists(
                    contracts, instance_values["contract"], user.id
                )
                contracts_cache.add(instance_values["contract"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("organization") is not None:
        if instance_values["organization"] not in organizations_cache:
            try:
                await check_entity_exists(
                    organizations, instance_values["organization"], user.id
                )
                organizations_cache.add(instance_values["organization"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("warehouse") is not None:
        if instance_values["warehouse"] not in warehouses_cache:
            try:
                await check_entity_exists(
                    warehouses, instance_values["warehouse"], user.id
                )
                warehouses_cache.add(instance_values["warehouse"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("purchased_by") is not None:
        if instance_values["purchased_by"] not in users_cache:
            query = users_cboxes_relation.select().where(
                users_cboxes_relation.c.id == instance_values["purchased_by"]
            )
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
        if not await check_period_blocked(
            instance_values["organization"], instance_values.get("dated"), exceptions
        ):
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
                        await check_entity_exists(
                            price_types, item["price_type"], user.id
                        )
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
                        warehouse_balances.c.nomenclature_id == item["nomenclature"]
                    )
                    .order_by(desc(warehouse_balances.c.created_at))
                )
                last_warehouse_balance = await database.fetch_one(query)
                warehouse_amount = (
                    last_warehouse_balance.current_amount
                    if last_warehouse_balance
                    else 0
                )

                query = warehouse_balances.insert().values(
                    {
                        "organization_id": instance_values["organization"],
                        "warehouse_id": instance_values["warehouse"],
                        "nomenclature_id": item["nomenclature"],
                        "document_purchase_id": instance_id,
                        "incoming_amount": item["quantity"],
                        "current_amount": warehouse_amount + item["quantity"],
                        "cashbox_id": user.id,
                    }
                )
                await database.execute(query)
        query = (
            docs_purchases.update()
            .where(docs_purchases.c.id == instance_id)
            .values({"sum": items_sum})
        )
        await database.execute(query)

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
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return docs_purchases_db


@router.patch("/docs_purchases/{idx}", response_model=schemas.ListView)
async def update(token: str, docs_purchases_data: schemas.EditMass):
    """Редактирование документов"""
    user = await get_user_by_token(token)

    updated_ids = set()
    exceptions = []
    for instance_values in docs_purchases_data.dict(exclude_unset=True)["__root__"]:
        if not await check_period_blocked(
            instance_values["organization"], instance_values.get("dated"), exceptions
        ):
            continue
        if not await check_foreign_keys(instance_values, user, exceptions):
            continue

        goods: list = instance_values.get("goods")
        try:
            del instance_values["goods"]
        except KeyError:
            pass
        query = (
            docs_purchases.update()
            .where(docs_purchases.c.id == instance_values["id"])
            .values(instance_values)
        )
        await database.execute(query)
        instance_id = instance_values["id"]
        updated_ids.add(instance_id)
        if goods:
            query = docs_purchases_goods.delete().where(
                docs_purchases_goods.c.docs_purchases_id == instance_id
            )
            await database.execute(query)
            items_sum = 0
            for item in goods:
                item["docs_purchases_id"] = instance_id

                if item.get("price_type") is not None:
                    if item["price_type"] not in price_types_cache:
                        try:
                            await check_entity_exists(
                                price_types, item["price_type"], user.id
                            )
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
                            warehouse_balances.c.nomenclature_id == item["nomenclature"]
                        )
                        .order_by(desc(warehouse_balances.c.created_at))
                    )
                    last_warehouse_balance = await database.fetch_one(query)
                    warehouse_amount = (
                        last_warehouse_balance.current_amount
                        if last_warehouse_balance
                        else 0
                    )

                    query = warehouse_balances.insert().values(
                        {
                            "organization_id": instance_values["organization"],
                            "warehouse_id": instance_values["warehouse"],
                            "nomenclature_id": item["nomenclature"],
                            "document_purchase_id": instance_id,
                            "incoming_amount": item["quantity"],
                            "current_amount": warehouse_amount + item["quantity"],
                            "cashbox_id": user.id,
                        }
                    )
                    await database.execute(query)

            query = (
                docs_purchases.update()
                .where(docs_purchases.c.id == instance_id)
                .values({"sum": items_sum})
            )
            await database.execute(query)

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
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return docs_purchases_db


@router.delete("/docs_purchases/{idx}", response_model=schemas.ListView)
async def delete(token: str, ids: list[int]):
    """Удаление документов"""
    await get_user_by_token(token)

    query = docs_purchases.select().where(
        docs_purchases.c.id.in_(ids), docs_purchases.c.is_deleted.is_not(True)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            docs_purchases.update()
            .where(
                docs_purchases.c.id.in_(ids), docs_purchases.c.is_deleted.is_not(True)
            )
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

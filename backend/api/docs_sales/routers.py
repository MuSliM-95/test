from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import desc

from database.db import (
    database,
    articles,
    payments,
    entity_to_entity,
    pboxes,
    docs_sales,
    organizations,
    docs_sales_goods,
    contracts,
    loyality_cards,
    loyality_transactions,
    warehouses,
    users_cboxes_relation,
    price_types, warehouse_balances,
)
import datetime

from api.loyality_transactions.routers import raschet_bonuses
from functions.users import raschet

import asyncio

from . import schemas

from functions.helpers import (
    datetime_to_timestamp,
    check_contragent_exists,
    check_entity_exists,
    add_nomenclature_name_to_goods,
    check_unit_exists,
    check_period_blocked,
    add_nomenclature_count,
    raschet_oplat
)
from functions.helpers import get_user_by_token

from ws_manager import manager

router = APIRouter(tags=["docs_sales"])

contragents_cache = set()
organizations_cache = set()
contracts_cache = set()
warehouses_cache = set()
users_cache = set()
price_types_cache = set()
units_cache = set()


@router.get("/docs_sales/{idx}", response_model=schemas.View)
async def get_by_id(token: str, idx: int):
    """Получение документа по ID"""
    await get_user_by_token(token)
    query = docs_sales.select().where(
        docs_sales.c.id == idx, docs_sales.c.is_deleted.is_not(True)
    )
    instance_db = await database.fetch_one(query)

    if not instance_db:
        raise HTTPException(status_code=404, detail=f"Не найдено.")

    instance_db = datetime_to_timestamp(instance_db)
    instance_db = await raschet_oplat(instance_db)

    query = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == idx)
    goods_db = await database.fetch_all(query)
    goods_db = [*map(datetime_to_timestamp, goods_db)]

    goods_db = [*map(add_nomenclature_name_to_goods, goods_db)]
    goods_db = [await instance for instance in goods_db]

    instance_db["goods"] = goods_db

    return instance_db


@router.get("/docs_sales/", response_model=schemas.CountRes)
async def get_list(token: str, limit: int = 100, offset: int = 0, filters: schemas.FilterSchema = Depends()):
    """Получение списка документов"""
    user = await get_user_by_token(token)
    query = (
        docs_sales.select()
        .where(docs_sales.c.is_deleted.is_not(True), docs_sales.c.cashbox == user.cashbox_id)
        .limit(limit)
        .offset(offset)
        .order_by(desc(docs_sales.c.id))
    )

    if filters.tags:
        query = query.filter(docs_sales.c.tags.like(f"%{filters.tags}%"))
    
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]
    items_db = [*map(add_nomenclature_count, items_db)]
    items_db = [await instance for instance in items_db]
    items_db = [*map(raschet_oplat, items_db)]
    items_db = [await instance for instance in items_db]

    count = len(items_db)

    return {"result": items_db, "count": count}


async def check_foreign_keys(instance_values, user, exceptions) -> bool:
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

    if instance_values.get("sales_manager") is not None:
        if instance_values["sales_manager"] not in users_cache:
            query = users_cboxes_relation.select().where(
                users_cboxes_relation.c.id == instance_values["sales_manager"]
            )
            if not await database.fetch_one(query):
                exceptions.append(str(instance_values) + " Пользователь не существует!")
                return False
            users_cache.add(instance_values["sales_manager"])
    return True


@router.post("/docs_sales/", response_model=schemas.ListView)
async def create(token: str, docs_sales_data: schemas.CreateMass):
    """Создание документов"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    exceptions = []

    q = docs_sales.select().where(
        docs_sales.c.cashbox == user.cashbox_id,
        docs_sales.c.status == True,
        docs_sales.c.is_deleted == False
    )
    docs_db = await database.fetch_all(q)
    number = len(docs_db) + 1

    for instance_values in docs_sales_data.dict()["__root__"]:
        instance_values["created_by"] = user.id
        instance_values["sales_manager"] = user.id
        instance_values["is_deleted"] = False
        instance_values["cashbox"] = user.cashbox_id

        if not instance_values['number']:
            instance_values['number'] = str(number)
            number += 1

        paid_rubles = instance_values["paid_rubles"]
        paid_lt = instance_values["paid_lt"]
        lt = instance_values["loyality_card_id"]

        del instance_values["paid_lt"]
        del instance_values["paid_rubles"]
        del instance_values["loyality_card_id"]
        

        # if paid_rubles 

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

        del instance_values["client"]

        goods: list = instance_values.get("goods")
        try:
            del instance_values["goods"]
        except KeyError:
            pass
        query = docs_sales.insert().values(instance_values)
        instance_id = await database.execute(query)
        inserted_ids.add(instance_id)
        items_sum = 0
        for item in goods:
            item["docs_sales_id"] = instance_id
            del item["nomenclature_name"]
            del item["unit_name"]

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
            query = docs_sales_goods.insert().values(item)
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
                        "document_sale_id": instance_id,
                        "outgoing_amount": item["quantity"],
                        "current_amount": warehouse_amount - item["quantity"],
                        "cashbox_id": user.cashbox_id,
                    }
                )
                await database.execute(query)

            
        if paid_rubles > 0:
            paybox_q = pboxes.select().where(pboxes.c.cashbox == user.cashbox_id)
            payboxes = await database.fetch_all(paybox_q)

            article_id = None

            article_q = articles.select().where(articles.c.cashbox == user.cashbox_id, articles.c.name == "Продажи")
            article_db = await database.fetch_one(article_q)

            if article_db:
                article_id = article_db.id
            else:
                tstamp = int(datetime.datetime.now().timestamp())
                created_article_q = articles.insert().values({
                    "name": "Продажи",
                    "emoji": "🛍️",
                    "cashbox": user.cashbox_id,
                    "created_at": tstamp,
                    "updated_at": tstamp
                })
                created_article_db = await database.execute(created_article_q)
                article_id = created_article_db

            rubles_body = {
                "contragent": instance_values['contragent'],
                "type": "incoming",
                "name": f"Оплата по документу {instance_values['number']}",
                "amount_without_tax": paid_rubles,
                "tags": instance_values.get("tags", ""),
                "amount": paid_rubles,
                "tax": 0,
                "tax_type": "internal",
                "article_id": article_id,
                "article": "Продажи",
                "paybox": payboxes[0].id,
                "date": int(datetime.datetime.now().timestamp()),
                "account": user.id,
                "cashbox": user.cashbox_id,
                "is_deleted": False,
                "created_at": int(datetime.datetime.now().timestamp()),
                "updated_at": int(datetime.datetime.now().timestamp()),
                "status": True,
                "stopped": True,
            }
            payment_id = await database.execute(payments.insert().values(rubles_body))

            await database.execute(entity_to_entity.insert().values(
                {
                    "from_entity": 7,
                    "to_entity": 5,
                    "cashbox_id": user.cashbox_id,
                    "type": "docs_sales_payments",
                    "from_id": instance_id,
                    "to_id": payment_id,
                    "status": True,
                    "delinked": False,
                }
            ))

            if lt:
                lcard_q = loyality_cards.select().where(loyality_cards.c.id == lt)
                lcard = await database.fetch_one(lcard_q)
                rubles_body = {
                    "loyality_card_id": lt,
                    "loyality_card_number": lcard.card_number,
                    "type": "accrual",
                    "name": f"Кешбек по документу {instance_values['number']}",
                    "amount": round((paid_rubles * (lcard.cashback_percent / 100)), 2),
                    "created_by_id": user.id,
                    "tags": "",
                    "dated": datetime.datetime.now(),
                    "cashbox": user.cashbox_id,
                    "is_deleted": False,
                    "created_at": datetime.datetime.now(),
                    "updated_at": datetime.datetime.now(),
                    "status": True,
                }
                lt_id = await database.execute(loyality_transactions.insert().values(rubles_body))
                asyncio.create_task(raschet_bonuses(user))

            asyncio.create_task(raschet(user, token))

        if lt:
            if paid_lt > 0:
                paybox_q = loyality_cards.select().where(loyality_cards.c.id == lt)
                payboxes = await database.fetch_one(paybox_q)

                rubles_body = {
                    "loyality_card_id": lt,
                    "loyality_card_number": payboxes.card_number,
                    "type": "withdraw",
                    "name": f"Оплата по документу {instance_values['number']}",
                    "amount": paid_lt,
                    "created_by_id": user.id,
                    "tags": "",
                    "dated": datetime.datetime.now(),
                    "cashbox": user.cashbox_id,
                    "is_deleted": False,
                    "created_at": datetime.datetime.now(),
                    "updated_at": datetime.datetime.now(),
                    "status": True,
                }
                lt_id = await database.execute(loyality_transactions.insert().values(rubles_body))

                await database.execute(entity_to_entity.insert().values(
                    {
                        "from_entity": 7,
                        "to_entity": 6,
                        "cashbox_id": user.cashbox_id,
                        "type": "docs_sales_loyality_transactions",
                        "from_id": instance_id,
                        "to_id": lt_id,
                        "status": True,
                        "delinked": False,
                    }
                ))

                asyncio.create_task(raschet_bonuses(user))

            

        query = (
            docs_sales.update()
            .where(docs_sales.c.id == instance_id)
            .values({"sum": items_sum})
        )
        await database.execute(query)

    query = docs_sales.select().where(docs_sales.c.id.in_(inserted_ids))
    docs_sales_db = await database.fetch_all(query)
    docs_sales_db = [*map(datetime_to_timestamp, docs_sales_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "docs_sales",
            "result": docs_sales_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return docs_sales_db


@router.patch("/docs_sales/{idx}", response_model=schemas.ListView)
async def update(token: str, docs_sales_data: schemas.EditMass):
    """Редактирование документов"""
    user = await get_user_by_token(token)

    updated_ids = set()
    exceptions = []
    for instance_values in docs_sales_data.dict(exclude_unset=True)["__root__"]:
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

        paid_rubles = None
        paid_lt = None
        lt = None
        
        if instance_values.get("paid_rubles") is not None:
            paid_rubles = instance_values["paid_rubles"]
            del instance_values["paid_rubles"]
            
        if instance_values.get("paid_lt") is not None:
            paid_lt = instance_values["paid_lt"]
            del instance_values["paid_lt"]
            
        if instance_values.get("loyality_card_id") is not None:
            lt = instance_values["loyality_card_id"]
            del instance_values["loyality_card_id"]

        instance_id_db = instance_values["id"]
        
        if paid_rubles or paid_lt or lt:
            
            query = (
                entity_to_entity.select()
                .where(entity_to_entity.c.cashbox_id == user.cashbox_id, entity_to_entity.c.from_id == instance_values["id"])
            )
            proxyes = await database.fetch_all(query)

            proxy_payment = False
            proxy_lt = False

            for proxy in proxyes:
                if proxy.from_entity == 7:

                    # Платеж

                    if proxy.to_entity == 5:
                        q_payment = payments.update().where(
                            payments.c.id == proxy.to_id,
                            payments.c.cashbox == user.cashbox_id,
                            payments.c.status == True, 
                            payments.c.is_deleted == False
                        ).values({ "amount": paid_rubles, "amount_without_tax": paid_rubles })
                        await database.execute(q_payment)
                        proxy_payment = True

                    # Транзакция
                    if proxy.to_entity == 6:
                        q_trans = loyality_transactions.update().where(
                            loyality_transactions.c.id == proxy.to_id,
                            loyality_transactions.c.cashbox == user.cashbox_id,
                            loyality_transactions.c.status == True, 
                            loyality_transactions.c.is_deleted == False
                        ).values({ "amount": paid_lt })
                        await database.execute(q_trans)
                        proxy_lt = True

            if not proxy_payment:
                paybox_q = pboxes.select().where(pboxes.c.cashbox == user.cashbox_id)
                payboxes = await database.fetch_all(paybox_q)

                rubles_body = {
                    "contragent": instance_values['contragent'],
                    "type": "outgoing",
                    "name": f"Оплата по документу {instance_values['number']}",
                    "amount_without_tax": instance_values.get("paid_rubles"),
                    "amount": instance_values.get("paid_rubles"),
                    "paybox": payboxes[0].id,
                    "date": int(datetime.datetime.now().timestamp()),
                    "account": user.id,
                    "cashbox": user.cashbox_id,
                    "is_deleted": False,
                    "created_at": int(datetime.datetime.now().timestamp()),
                    "updated_at": int(datetime.datetime.now().timestamp()),
                    "status": True,
                    "stopped": True,
                }
                payment_id = await database.execute(payments.insert().values(rubles_body))

                await database.execute(entity_to_entity.insert().values(
                    {
                        "from_entity": 7,
                        "to_entity": 5,
                        "cashbox_id": user.cashbox_id,
                        "type": "docs_sales_payments",
                        "from_id": instance_id_db,
                        "to_id": payment_id,
                        "status": True,
                        "delinked": False,
                    }
                ))

                if lt:
                    lcard_q = loyality_cards.select().where(loyality_cards.c.id == lt)
                    lcard = await database.fetch_one(lcard_q)
                    rubles_body = {
                        "loyality_card_id": lt,
                        "loyality_card_number": lcard.card_number,
                        "type": "accrual",
                        "name": f"Кешбек по документу {instance_values['number']}",
                        "amount": round((paid_rubles * (lcard.cashback_percent / 100)), 2),
                        "created_by_id": user.id,
                        "tags": "",
                        "dated": datetime.datetime.now(),
                        "cashbox": user.cashbox_id,
                        "is_deleted": False,
                        "created_at": datetime.datetime.now(),
                        "updated_at": datetime.datetime.now(),
                        "status": True,
                    }
                    lt_id = await database.execute(loyality_transactions.insert().values(rubles_body))
                    asyncio.create_task(raschet_bonuses(user))

                asyncio.create_task(raschet(user, token))

            if lt and not proxy_lt:
                if paid_lt > 0:
                    paybox_q = loyality_cards.select().where(loyality_cards.c.id == lt)
                    payboxes = await database.fetch_one(paybox_q)

                    rubles_body = {
                        "loyality_card_id": lt,
                        "loyality_card_number": payboxes.card_number,
                        "type": "withdraw",
                        "name": f"Оплата по документу {instance_values['number']}",
                        "amount": paid_lt,
                        "created_by_id": user.id,
                        "tags": "",
                        "dated": datetime.datetime.now(),
                        "cashbox": user.cashbox_id,
                        "is_deleted": False,
                        "created_at": datetime.datetime.now(),
                        "updated_at": datetime.datetime.now(),
                        "status": True,
                    }
                    lt_id = await database.execute(loyality_transactions.insert().values(rubles_body))

                    await database.execute(entity_to_entity.insert().values(
                        {
                            "from_entity": 7,
                            "to_entity": 6,
                            "cashbox_id": user.cashbox_id,
                            "type": "docs_sales_loyality_transactions",
                            "from_id": instance_id_db,
                            "to_id": lt_id,
                            "status": True,
                            "delinked": False,
                        }
                    ))

                    asyncio.create_task(raschet_bonuses(user))

        query = (
            docs_sales.update()
            .where(docs_sales.c.id == instance_values["id"])
            .values(instance_values)
        )
        await database.execute(query)
        instance_id = instance_values["id"]
        updated_ids.add(instance_id)
        if goods:
            query = docs_sales_goods.delete().where(
                docs_sales_goods.c.docs_sales_id == instance_id
            )
            await database.execute(query)
            items_sum = 0
            for item in goods:
                item["docs_sales_id"] = instance_id

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
                query = docs_sales_goods.insert().values(item)
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
                            "document_sale_id": instance_id,
                            "outgoing_amount": item["quantity"],
                            "current_amount": warehouse_amount - item["quantity"],
                            "cashbox_id": user.id,
                        }
                    )
                    await database.execute(query)

            query = (
                docs_sales.update()
                .where(docs_sales.c.id == instance_id)
                .values({"sum": items_sum})
            )
            await database.execute(query)

    query = docs_sales.select().where(docs_sales.c.id.in_(updated_ids))
    docs_sales_db = await database.fetch_all(query)
    docs_sales_db = [*map(datetime_to_timestamp, docs_sales_db)]

    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "docs_sales",
            "result": docs_sales_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return docs_sales_db


@router.delete("/docs_sales/", response_model=schemas.ListView)
async def delete(token: str, ids: list[int]):
    """Пакетное удаление документов"""
    await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id.in_(ids), docs_sales.c.is_deleted.is_not(True)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            docs_sales.update()
            .where(docs_sales.c.id.in_(ids), docs_sales.c.is_deleted.is_not(True))
            .values({"is_deleted": True})
        )
        await database.execute(query)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_sales",
                "result": items_db,
            },
        )

    return items_db


@router.delete("/docs_sales/{idx}", response_model=schemas.ListView)
async def delete(token: str, idx: int):
    """Удаление документа"""
    await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx, docs_sales.c.is_deleted.is_not(True)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            docs_sales.update()
            .where(docs_sales.c.id == idx, docs_sales.c.is_deleted.is_not(True))
            .values({"is_deleted": True})
        )
        await database.execute(query)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_sales",
                "result": items_db,
            },
        )

    return items_db

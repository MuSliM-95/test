import os
from typing import Union,Dict,Any,Optional,Literal

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import desc, func, and_, select
from sqlalchemy.dialects import postgresql

from apps.yookassa.functions.core.IGetOauthCredentialFunction import IGetOauthCredentialFunction
from apps.yookassa.functions.impl.GetOauthCredentialFunction import GetOauthCredentialFunction
from apps.yookassa.models.PaymentModel import PaymentCreateModel,AmountModel,ReceiptModel,CustomerModel,ItemModel,\
    ConfirmationRedirect
from apps.yookassa.repositories.core.IYookassaOauthRepository import IYookassaOauthRepository
from apps.yookassa.repositories.core.IYookassaRequestRepository import IYookassaRequestRepository
from apps.yookassa.repositories.impl.YookassaCrmPaymentsRepository import YookassaCrmPaymentsRepository
from apps.yookassa.repositories.impl.YookassaOauthRepository import YookassaOauthRepository
from apps.yookassa.repositories.impl.YookassaPaymentsRepository import YookassaPaymentsRepository
from apps.yookassa.repositories.impl.YookassaRequestRepository import YookassaRequestRepository
from apps.yookassa.services.impl.OauthService import OauthService
from apps.yookassa.services.impl.YookassaApiService import YookassaApiService
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
    nomenclature,
    docs_warehouse, docs_sales_tags, amo_leads, amo_install_table_cashboxes, amo_leads_docs_sales_mapping,
    docs_sales_settings, contragents,

)
import datetime

from api.loyality_transactions.routers import raschet_bonuses
from functions.users import raschet

import asyncio

from . import schemas

from api.docs_warehouses.utils import create_warehouse_docs
from api.docs_warehouses.routers import update as update_warehouse_doc
from api.docs_warehouses.schemas import EditMass as WarehouseUpdate

from functions.helpers import (
    datetime_to_timestamp,
    check_contragent_exists,
    check_entity_exists,
    add_nomenclature_name_to_goods,
    check_unit_exists,
    check_period_blocked,
    add_nomenclature_count,
    raschet_oplat,
    add_docs_sales_settings
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

async def exists_settings_docs_sales(docs_sales_id: int) -> bool:
    query = (
        docs_sales
        .select()
        .where(
            docs_sales.c.id == docs_sales_id,
            docs_sales.c.settings.is_not(None)
        )
    )
    exists = await database.fetch_one(query)
    return bool(exists)


async def add_settings_docs_sales(settings: Optional[dict]) -> Optional[int]:
    if settings:
        query = (
            docs_sales_settings
            .insert()
            .values(settings)
        )
        docs_sales_settings_id = await database.execute(query)
        return docs_sales_settings_id


async def update_settings_docs_sales(docs_sales_id: int, settings: Optional[dict]) -> None:
    if settings:
        docs_sales_ids = (
            select(docs_sales.c.settings)
            .where(docs_sales.c.id == docs_sales_id)
            .subquery("docs_sales_ids")
        )
        query = (
            docs_sales_settings
            .update()
            .where(docs_sales_settings.c.id.in_(docs_sales_ids))
            .values(settings)
        )
        await database.execute(query)


@router.get("/docs_sales/{idx}/", response_model=schemas.View)
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
    instance_db = await add_docs_sales_settings(instance_db)

    query = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == idx)
    goods_db = await database.fetch_all(query)
    goods_db = [*map(datetime_to_timestamp, goods_db)]

    goods_db = [*map(add_nomenclature_name_to_goods, goods_db)]
    goods_db = [await instance for instance in goods_db]

    instance_db["goods"] = goods_db

    return instance_db


@router.get("/docs_sales/", response_model=schemas.CountRes)
async def get_list(token: str, limit: int = 100, offset: int = 0, show_goods: bool = False,
                   filters: schemas.FilterSchema = Depends()):
    """Получение списка документов"""
    user = await get_user_by_token(token)

    query = (
        select(
            *docs_sales.columns,
            contragents.c.name.label("contragent_name")
        )
        .select_from(docs_sales)
        .outerjoin(contragents, docs_sales.c.contragent == contragents.c.id)
        .where(
            docs_sales.c.is_deleted.is_not(True),
            docs_sales.c.cashbox == user.cashbox_id
        )
        .limit(limit)
        .offset(offset)
        .order_by(desc(docs_sales.c.id))
    )
    count_query = (
        select(func.count())
        .select_from(docs_sales)
        .where(docs_sales.c.is_deleted.is_not(True), docs_sales.c.cashbox == user.cashbox_id)
    )

    filters_dict = filters.dict(exclude_none=True)
    filter_list = []
    for k, v in filters_dict.items():
        if k.split("_")[-1] == "from":
            dated_from_param_value = func.to_timestamp(v)
            creation_date = func.to_timestamp(docs_sales.c.dated)
            dated_to_param_value = func.to_timestamp(
                filters_dict.get(k.replace("from", "to"))
            )
            filter_list.append(
                and_(
                    dated_from_param_value <= creation_date,
                    creation_date <= dated_to_param_value,
                )
            )

        elif k.split("_")[-1] == "to":
            continue

        elif type(v) is bool:
            filter_list.append(and_(eval(f"docs_sales.c.{k}.is_({v})")))

        elif type(v) is str:
            filter_list.append(
                and_(*list(
                    map(
                        lambda x: eval(f"docs_sales.c.{k}.ilike(f'%{x.strip().lower()}%')"),
                        v.strip().split(',')
                    )
                ))
            )
        else:
            filter_list.append(and_(eval(f"docs_sales.c.{k} == {v}")))

    query = query.filter(and_(*filter_list))

    count_query = count_query.filter(and_(*filter_list))

    items_db = await database.fetch_all(query)
    count = await database.fetch_val(count_query)

    items_db = [*map(datetime_to_timestamp, items_db)]

    doc_ids = [item["id"] for item in items_db]

    goods_query = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id.in_(doc_ids))
    goods_data = await database.fetch_all(goods_query)
    goods_map = {}

    for good in goods_data:
        doc_id = good["docs_sales_id"]
        if doc_id not in goods_map:
            goods_map[doc_id] = []
        goods_map[doc_id].append(good)

    for item in items_db:
        goods = goods_map.get(item["id"], [])
        item["nomenclature_count"] = len(goods)
        item["doc_discount"] = round(sum(good.get("sum_discounted", 0) or 0 for good in goods), 2)

    settings_ids = [item["settings"] for item in items_db]
    settings_query = docs_sales_settings.select().where(docs_sales_settings.c.id.in_(settings_ids))
    settings_data = await database.fetch_all(settings_query)
    settings_map = {setting["id"]: setting for setting in settings_data}

    for item in items_db:
        item["settings"] = settings_map.get(item["settings"])

    items_db = await asyncio.gather(*[raschet_oplat(item) for item in items_db])

    items_db = [*map(raschet_oplat, items_db)]
    items_db = [await instance for instance in items_db]

    if show_goods:
        for item in items_db:
            item_goods = goods_map.get(item["id"], [])
            item["goods"] = [add_nomenclature_name_to_goods(good) for good in item_goods]

    if show_goods:
        for item in items_db:
            query = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == item['id'])
            goods_db = await database.fetch_all(query)
            goods_db = [*map(datetime_to_timestamp, goods_db)]

            goods_db = await asyncio.gather(*[add_nomenclature_name_to_goods(good) for good in goods_db])

            item['goods'] = goods_db

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
async def create(token: str, docs_sales_data: schemas.CreateMass, generate_out: bool = True):
    """Создание документов"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    exceptions = []

    count_query = (
        select(func.count(docs_sales.c.id))
        .where(
            docs_sales.c.cashbox == user.cashbox_id,
            docs_sales.c.is_deleted.is_(False)
        )
    )
    count_docs_sales = await database.fetch_val(count_query, column=0)

    paybox_q = pboxes.select().where(pboxes.c.cashbox == user.cashbox_id)
    paybox = await database.fetch_one(paybox_q)
    paybox_id = None if not paybox else paybox.id

    article_q = articles.select().where(articles.c.cashbox == user.cashbox_id, articles.c.name == "Продажи")
    article_db = await database.fetch_one(article_q)

    for index, instance_values in enumerate(docs_sales_data.dict()["__root__"]):
        instance_values["created_by"] = user.id
        instance_values["sales_manager"] = user.id
        instance_values["is_deleted"] = False
        instance_values["cashbox"] = user.cashbox_id
        instance_values["settings"] = await add_settings_docs_sales(instance_values.pop("settings", None))

        goods: Union[list, None] = instance_values.pop("goods", None)

        goods_tmp = goods

        paid_rubles = instance_values.pop("paid_rubles", 0)
        paid_rubles = 0 if not paid_rubles else paid_rubles

        paid_lt = instance_values.pop("paid_lt", 0)
        paid_lt = 0 if not paid_lt else paid_lt

        lt = instance_values.pop("loyality_card_id")

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

        if not instance_values.get("number"):
            query = (
                select(docs_sales.c.number)
                .where(
                    docs_sales.c.is_deleted == False,
                    docs_sales.c.organization == instance_values["organization"]
                )
                .order_by(desc(docs_sales.c.created_at))
            )
            prev_number_docs_sales = await database.fetch_one(query)
            if prev_number_docs_sales:
                if prev_number_docs_sales.number:
                    try:
                        number_int = int(prev_number_docs_sales.number)
                    except:
                        number_int = 0
                    instance_values["number"] = str(number_int + 1)
                else:
                    instance_values["number"] = "1"
            else:
                instance_values["number"] = "1"

        paybox = instance_values.pop('paybox', None)
        if paybox is None:
            if paybox_id is not None:
                paybox = paybox_id

        query = docs_sales.insert().values(instance_values)
        instance_id = await database.execute(query)

        # Процесс разделения тегов(в другую таблицу)
        tags = instance_values.pop("tags", "")
        if tags:
            tags_insert_list = []
            tags_split = tags.split(",")
            for tag_name in tags_split:
                tags_insert_list.append({
                    "docs_sales_id": instance_id,
                    "name": tag_name,
                })
            if tags_insert_list:
                await database.execute(docs_sales_tags.insert(tags_insert_list))

        inserted_ids.add(instance_id)
        items_sum = 0

        cashback_sum = 0

        lcard = None
        if lt:
            lcard_q = loyality_cards.select().where(loyality_cards.c.id == lt)
            lcard = await database.fetch_one(lcard_q)

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
            item["nomenclature"] = int(item["nomenclature"])
            query = docs_sales_goods.insert().values(item)
            await database.execute(query)

            items_sum += item["price"] * item["quantity"]

            if lcard:
                nomenclature_db = await database.fetch_one(nomenclature.select().where(nomenclature.c.id == item['nomenclature']))
                if nomenclature_db:
                    if nomenclature_db.cashback_percent is not None:
                        if nomenclature_db.cashback_percent != 0:
                            cashback_sum += item["price"] * item["quantity"] * (nomenclature_db.cashback_percent / 100)
                    else:
                        cashback_sum += item["price"] * item["quantity"] * (lcard.cashback_percent / 100)
                else:
                    cashback_sum += item["price"] * item["quantity"] * (lcard.cashback_percent / 100)

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
                article_id = await database.execute(created_article_q)

            payment_id = await database.execute(payments.insert().values({
                "contragent": instance_values['contragent'],
                "type": "incoming",
                "name": f"Оплата по документу {instance_values['number']}",
                "amount_without_tax": round(paid_rubles, 2),
                "tags": tags,
                "amount": round(paid_rubles, 2),
                "tax": 0,
                "tax_type": "internal",
                "article_id": article_id,
                "article": "Продажи",
                "paybox": paybox,
                "date": int(datetime.datetime.now().timestamp()),
                "account": user.user,
                "cashbox": user.cashbox_id,
                "is_deleted": False,
                "created_at": int(datetime.datetime.now().timestamp()),
                "updated_at": int(datetime.datetime.now().timestamp()),
                "status": instance_values['status'],
                "stopped": True,
                "docs_sales_id": instance_id
            }))
            await database.execute(
                pboxes.update().where(pboxes.c.id == paybox).values({"balance": pboxes.c.balance - paid_rubles})
            )

            # Юкасса

            yookassa_oauth_service = OauthService(
                oauth_repository = YookassaOauthRepository(),
                request_repository = YookassaRequestRepository(),
                get_oauth_credential_function = GetOauthCredentialFunction()
            )

            yookassa_api_service = YookassaApiService(
                request_repository = YookassaRequestRepository(),
                oauth_repository = YookassaOauthRepository(),
                payments_repository = YookassaPaymentsRepository(),
                crm_payments_repository = YookassaCrmPaymentsRepository()
            )

            if await yookassa_oauth_service.validation_oauth(user.cashbox_id,instance_values['warehouse']):
                await yookassa_api_service.api_create_payment(
                    user.cashbox_id,
                    instance_values['warehouse'],
                    instance_id,
                    payment_id,
                    PaymentCreateModel(
                        amount = AmountModel(
                            value = str(round(paid_rubles, 2)),
                            currency = "RUB"
                        ),
                        description = f"Оплата по документу {instance_values['number']}",
                        capture = True,
                        receipt = ReceiptModel(
                            customer = CustomerModel(),
                            items = [ItemModel(
                                description = good.get("nomenclature_name") or "",
                                amount = AmountModel(
                                    value = good.get("price"),
                                    currency = "RUB"
                                ),
                                quantity = good.get("quantity"),
                                vat_code = "1"
                            ) for good in goods_tmp],
                        ),
                        confirmation = ConfirmationRedirect(
                            type = "redirect",
                            return_url = f"https://${os.getenv('APP_URL')}/?token=${token}"
                        )
                    )
                )

            # юкасса

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
            if lcard:
                if cashback_sum > 0:
                    calculated_share = paid_rubles / (paid_rubles + paid_lt)
                    calculated_cashback_sum = round((calculated_share * cashback_sum), 2)

                    if calculated_cashback_sum > 0:
                        rubles_body = {
                            "loyality_card_id": lt,
                            "loyality_card_number": lcard.card_number,
                            "type": "accrual",
                            "name": f"Кешбек по документу {instance_values['number']}",
                            "amount": calculated_cashback_sum,
                            "created_by_id": user.id,
                            "tags": tags,
                            "card_balance": lcard.balance,
                            "dated": datetime.datetime.now(),
                            "cashbox": user.cashbox_id,
                            "is_deleted": False,
                            "created_at": datetime.datetime.now(),
                            "updated_at": datetime.datetime.now(),
                            "status": True,
                        }

                        lt_id = await database.execute(loyality_transactions.insert().values(rubles_body))

                        await asyncio.gather(asyncio.create_task(raschet_bonuses(lt)))

            await asyncio.gather(asyncio.create_task(raschet(user, token)))
        if lt:
            if paid_lt > 0:
                paybox_q = loyality_cards.select().where(loyality_cards.c.id == lt)
                payboxes = await database.fetch_one(paybox_q)
                print("loyality_transactions insert")
                rubles_body = {
                    "loyality_card_id": lt,
                    "loyality_card_number": payboxes.card_number,
                    "type": "withdraw",
                    "name": f"Оплата по документу {instance_values['number']}",
                    "amount": paid_lt,
                    "created_by_id": user.id,
                    "card_balance": lcard.balance,
                    "tags": tags,
                    "dated": datetime.datetime.now(),
                    "cashbox": user.cashbox_id,
                    "is_deleted": False,
                    "created_at": datetime.datetime.now(),
                    "updated_at": datetime.datetime.now(),
                    "status": True,
                }
                print("loyality_transactions insert")
                lt_id = await database.execute(loyality_transactions.insert().values(rubles_body))
                print("loyality_transactions insert")
                await database.execute(
                    loyality_cards.update() \
                        .where(loyality_cards.c.card_number == payboxes.card_number) \
                        .values({"balance": loyality_cards.c.balance - paid_lt})
                )
                print("loyality_transactions update")
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

                await asyncio.gather(asyncio.create_task(raschet_bonuses(lt)))

        query = (
            docs_sales.update()
            .where(docs_sales.c.id == instance_id)
            .values({"sum": round(items_sum, 2)})
        )
        await database.execute(query)

        if generate_out:
            goods_res = []
            for good in goods:
                nomenclature_id = int(good['nomenclature'])
                nomenclature_db = await database.fetch_one(
                    nomenclature.select().where(nomenclature.c.id == nomenclature_id))
                if nomenclature_db.type == "product":
                    goods_res.append(
                        {
                            "price_type": 1,
                            "price": 0,
                            "quantity": good['quantity'],
                            "unit": good['unit'],
                            "nomenclature": nomenclature_id
                        }
                    )

            body = {
                "number": None,
                "dated": instance_values['dated'],
                "docs_purchases": None,
                "to_warehouse": None,
                "status": True,
                "contragent": instance_values['contragent'],
                "organization": instance_values['organization'],
                "operation": "outgoing",
                "comment": instance_values['comment'],
                "warehouse": instance_values['warehouse'],
                "docs_sales_id": instance_id,
                "goods": goods_res
            }
            body['docs_purchases'] = None
            body['number'] = None
            body['to_warehouse'] = None
            await create_warehouse_docs(token, body, user.cashbox_id)



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


@router.patch("/docs_sales/{idx}/", response_model=schemas.ListView)
async def update(token: str, docs_sales_data: schemas.EditMass):
    """Редактирование документов"""
    user = await get_user_by_token(token)

    updated_ids = set()
    exceptions = []

    count_query = (
        select(func.count(docs_sales.c.id))
        .where(
            docs_sales.c.cashbox == user.cashbox_id,
            docs_sales.c.is_deleted.is_(False)
        )
    )

    count_docs_sales = await database.fetch_val(count_query, column=0)

    for index, instance_values in enumerate(docs_sales_data.dict(exclude_unset=True)["__root__"]):
        if not await check_period_blocked(
                instance_values["organization"], instance_values.get("dated"), exceptions
        ):
            continue

        if not await check_foreign_keys(instance_values, user, exceptions):
            continue

        # if instance_values.get("number") is None:
        #     instance_values["number"] = str(count_docs_sales + index + 1)

        goods: Union[list, None] = instance_values.pop("goods", None)

        paid_rubles = instance_values.pop("paid_rubles", 0)
        paid_lt = instance_values.pop("paid_lt", 0)
        lt = instance_values.pop("loyality_card_id", None)

        paybox = instance_values.pop('paybox', None)
        if paybox is None:
            paybox_q = pboxes.select().where(pboxes.c.cashbox == user.cashbox_id)
            paybox = await database.fetch_one(paybox_q)
            if paybox:
                paybox = paybox.id

        instance_id_db = instance_values["id"]

        settings: Optional[Dict[str, Any]] = instance_values.pop("settings", None)
        if await exists_settings_docs_sales(instance_id_db):
            await update_settings_docs_sales(instance_id_db, settings)
        else:
            instance_values["settings"] = await add_settings_docs_sales(settings)

        if paid_rubles or paid_lt or lt:
            query = (
                entity_to_entity.select()
                .where(entity_to_entity.c.cashbox_id == user.cashbox_id,
                       entity_to_entity.c.from_id == instance_values["id"])
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
                        ).values({"amount": paid_rubles, "amount_without_tax": paid_rubles})
                        await database.execute(q_payment)
                        proxy_payment = True

                    # Транзакция
                    if proxy.to_entity == 6:
                        q_trans = loyality_transactions.update().where(
                            loyality_transactions.c.id == proxy.to_id,
                            loyality_transactions.c.cashbox == user.cashbox_id,
                            loyality_transactions.c.status == True,
                            loyality_transactions.c.is_deleted == False
                        ).values({"amount": paid_lt})
                        await database.execute(q_trans)
                        proxy_lt = True

            if not proxy_payment:
                rubles_body = {
                    "contragent": instance_values['contragent'],
                    "type": "outgoing",
                    "name": f"Оплата по документу {instance_values['number']}",
                    "amount_without_tax": instance_values.get("paid_rubles"),
                    "amount": instance_values.get("paid_rubles"),
                    "paybox": paybox,
                    "tags": instance_values.get("tags", ""),
                    "date": int(datetime.datetime.now().timestamp()),
                    "account": user.user,
                    "cashbox": user.cashbox_id,
                    "is_deleted": False,
                    "created_at": int(datetime.datetime.now().timestamp()),
                    "updated_at": int(datetime.datetime.now().timestamp()),
                    "status": True,
                    "stopped": True,
                    "docs_sales_id": instance_id_db
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
                        "card_balance": lcard.balance,
                        "tags": instance_values.get("tags", ""),
                        "dated": datetime.datetime.now(),
                        "cashbox": user.cashbox_id,
                        "is_deleted": False,
                        "created_at": datetime.datetime.now(),
                        "updated_at": datetime.datetime.now(),
                        "status": True,
                    }
                    lt_id = await database.execute(loyality_transactions.insert().values(rubles_body))
                    await asyncio.gather(asyncio.create_task(raschet_bonuses(lt)))

                await asyncio.gather(asyncio.create_task(raschet(user, token)))

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
                        "tags": instance_values.get("tags", ""),
                        "dated": datetime.datetime.now(),
                        "card_balance": lcard.balance,
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

                    await asyncio.gather(asyncio.create_task(raschet_bonuses(lt)))

        if instance_values.get("paid_rubles"):
            del instance_values['paid_rubles']

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
                item["nomenclature"] = int(item["nomenclature"])
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

            query = (
                docs_sales.update()
                .where(docs_sales.c.id == instance_id)
                .values({"sum": round(items_sum, 2)})
            )
            await database.execute(query)

            doc_warehouse = await database.fetch_one(
                docs_warehouse.select().where(docs_warehouse.c.docs_sales_id == instance_id).order_by(
                    desc(docs_warehouse.c.id)))

            goods_res = []
            for good in goods:
                nomenclature_db = await database.fetch_one(
                    nomenclature.select().where(nomenclature.c.id == good['nomenclature']))
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

            body = WarehouseUpdate(__root__=[
                {
                    "id": doc_warehouse.id,
                    "number": None,
                    "dated": instance_values.get('dated'),
                    "docs_purchases": None,
                    "to_warehouse": None,
                    "status": True,
                    "contragent": instance_values['contragent'],
                    "operation": "outgoing",
                    "comment": instance_values['comment'],
                    "warehouse": instance_values['warehouse'],
                    "docs_sales_id": instance_id,
                    "goods": goods_res
                }
            ])

            await update_warehouse_doc(token, body)

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


@router.delete("/docs_sales/{idx}/", response_model=schemas.ListView)
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

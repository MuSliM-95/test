import asyncio
import os
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Set, Any
import decimal

from fastapi import HTTPException
from sqlalchemy import select, and_, or_, desc, func, cast, Integer

from api.docs_sales import schemas
# from api.docs_sales.events.financials.messages.RecalculateFinancialsMessageModel import \
#     RecalculateFinancialsMessageModel
# from api.docs_sales.events.loyalty_points.messages.RecalculateLoyaltyPointsMessageModel import \
#     RecalculateLoyaltyPointsMessageModel

from api.docs_sales.messages.TechCardWarehouseOperationMessage import TechCardWarehouseOperationMessage

from api.docs_warehouses.utils import create_warehouse_docs
from api.loyality_transactions.routers import raschet_bonuses
from apps.yookassa.functions.impl.GetOauthCredentialFunction import GetOauthCredentialFunction
from apps.yookassa.models.PaymentModel import PaymentCreateModel,AmountModel,ReceiptModel,CustomerModel,ItemModel,\
    ConfirmationRedirect
from apps.yookassa.repositories.core.IYookassaTableNomenclature import IYookassaTableNomenclature
from apps.yookassa.repositories.impl.YookassaCrmPaymentsRepository import YookassaCrmPaymentsRepository
from apps.yookassa.repositories.impl.YookassaOauthRepository import YookassaOauthRepository
from apps.yookassa.repositories.impl.YookassaPaymentsRepository import YookassaPaymentsRepository
from apps.yookassa.repositories.impl.YookassaRequestRepository import YookassaRequestRepository
from apps.yookassa.repositories.impl.YookassaTableNomenclature import YookassaTableNomenclature
from apps.yookassa.repositories.impl.YookasssaAmoTableCrmRepository import YookasssaAmoTableCrmRepository
from apps.yookassa.services.impl.OauthService import OauthService
from apps.yookassa.services.impl.YookassaApiService import YookassaApiService
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from database.db import database, contragents, contracts, organizations, warehouses, users_cboxes_relation, \
    nomenclature, price_types, units, articles, pboxes, fifo_settings, docs_sales, loyality_cards, docs_sales_goods, \
    docs_sales_tags, payments, loyality_transactions, entity_to_entity, warehouse_balances, docs_sales_settings, \
    NomenclatureCashbackType
from functions.helpers import get_user_by_token, datetime_to_timestamp
from functions.users import raschet
from ws_manager import manager
import phonenumbers


class CreateDocsSalesView:

    def __init__(
        self,
        rabbitmq_messaging_factory: IRabbitFactory
    ):
        self.__rabbitmq_messaging_factory = rabbitmq_messaging_factory

    async def __call__(
        self,
        token: str,
        docs_sales_data: schemas.CreateMass,
        generate_out: bool = True
    ):
        rabbitmq_messaging: IRabbitMessaging = await self.__rabbitmq_messaging_factory()
        user = await get_user_by_token(token)

        fks = defaultdict(set)
        nomenclature_ids = set()
        card_info: dict[int, dict[str, Any]] = {}

        for d in docs_sales_data.__root__:
            if d.loyality_card_id:
                fks["loyality_cards"].add(d.loyality_card_id)
            if d.contragent:
                fks["contr"].add(d.contragent)
            if d.contract:
                fks["contract"].add(d.contract)
            fks["org"].add(d.organization)
            if d.warehouse:
                fks["wh"].add(d.warehouse)
            if d.sales_manager:
                fks["mgr"].add(d.sales_manager)
            for g in d.goods or []:
                nomenclature_ids.add(int(g.nomenclature))
                if g.price_type:
                    fks["price"].add(g.price_type)
                if g.unit:
                    fks["unit"].add(g.unit)

        await self._validate_fk(contragents, fks["contr"], "contragents")
        await self._validate_fk(contracts, fks["contract"], "contracts")
        await self._validate_fk(organizations, fks["org"], "organizations")
        await self._validate_fk(warehouses, fks["wh"], "warehouses")
        await self._validate_fk(users_cboxes_relation, fks["mgr"], "sales_manager")
        await self._validate_fk(nomenclature, nomenclature_ids, "nomenclature")
        await self._validate_fk(price_types, fks["price"], "price_types")
        await self._validate_fk(units, fks["unit"], "units")

        conds = []
        for d in docs_sales_data.__root__:
            if d.dated is not None:
                conds.append(and_(fifo_settings.c.organization_id == d.organization, fifo_settings.c.blocked_date >= d.dated))
        if conds:
            blocked = await database.fetch_all(
                select(fifo_settings.c.organization_id, fifo_settings.c.blocked_date).where(or_(*conds)))
            if blocked:
                bad_orgs = {b.organization_id for b in blocked}
                raise HTTPException(400, f"Период закрыт для организаций: {', '.join(map(str, bad_orgs))}")

        article_id = await database.fetch_val(
            select(articles.c.id)
            .where(articles.c.cashbox == user.cashbox_id, articles.c.name == "Продажи")
            .limit(1),
            column=0,
        )
        if article_id is None:
            now_ts = int(datetime.now().timestamp())
            article_id = await database.execute(
                articles.insert().values(
                    name="Продажи",
                    emoji="🛍️",
                    cashbox=user.cashbox_id,
                    created_at=now_ts,
                    updated_at=now_ts,
                )
            )
        default_paybox = await database.fetch_val(
            select(pboxes.c.id).where(pboxes.c.cashbox == user.cashbox_id).limit(1),
            column=0,
        )
        if not default_paybox:
            raise HTTPException(404, f"Paybox Not Found")

        if fks["loyality_cards"]:
            rows = await database.fetch_all(
                select(
                    loyality_cards.c.id,
                    loyality_cards.c.card_number,
                    loyality_cards.c.balance,
                    loyality_cards.c.cashback_percent,
                ).where(loyality_cards.c.id.in_(fks["loyality_cards"]))
            )
            card_info = {r.id: {
                "number": r.card_number,
                "balance": r.balance,
                "percent": r.cashback_percent or 0,
            } for r in rows}

        nom_rows = await database.fetch_all(
            select(
                nomenclature.c.id,
                nomenclature.c.cashback_type,
                nomenclature.c.cashback_value,
                nomenclature.c.type,
            ).where(nomenclature.c.id.in_(nomenclature_ids))
        )
        nomenclature_cache = {r.id: r for r in nom_rows}

        card_withdraw_total: dict[int, float] = defaultdict(float)
        card_accrual_total: dict[int, float] = defaultdict(float)

        docs_rows, goods_rows, tags_rows = [], [], []
        payments_rows, lt_rows, e2e_rows = [], [], []
        wb_rows_dict: dict[tuple[int, int, int], float] = {}
        out_docs: list[tuple[dict, list[dict]]] = []

        settings_payload: list[dict | None] = []

        subq = (
            select(
                docs_sales.c.organization,
                docs_sales.c.number,
                func.row_number()
                .over(partition_by=docs_sales.c.organization,
                      order_by=docs_sales.c.id.desc())
                .label("rn")
            )
            .where(
                docs_sales.c.organization.in_(fks["org"]),
                docs_sales.c.is_deleted.is_(False),
            )
        ).subquery()

        last_number_rows = await database.fetch_all(
            select(subq.c.organization, subq.c.number)
            .where(subq.c.rn == 1)
        )

        last_number_by_org: dict[int, str] = {
            r.organization: (r.number or "") for r in last_number_rows
        }

        for idx, doc in enumerate(docs_sales_data.__root__):
            if doc.settings:
                raw = doc.settings.dict(exclude_unset=True) or {}
                if raw.get("repeatability_value") is not None and raw.get("repeatability_period") is not None:
                    settings_payload.append(raw)
                else:
                    settings_payload.append(None)
            else:
                settings_payload.append(None)

            if doc.number:
                number_str = doc.number
            else:
                last_raw = last_number_by_org.get(doc.organization, "")
                trimmed = (last_raw or "").strip()

                if trimmed.isdigit():
                    number_str = str(int(trimmed) + 1)
                else:
                    number_str = "1"

                last_number_by_org[doc.organization] = number_str

            if doc.priority is not None and (doc.priority < 0 or doc.priority > 10):
                raise HTTPException(400, "Приоритет должен быть от 0 до 10")   

            doc_dict = {
                "number": number_str,
                "dated": doc.dated,
                "operation": doc.operation,
                "tags": doc.tags,
                "parent_docs_sales": doc.parent_docs_sales,
                "comment": doc.comment,
                "contragent": doc.contragent,
                "contract": doc.contract,
                "organization": doc.organization,
                "sum": 0.0,
                "warehouse": doc.warehouse,
                "status": doc.status,
                "created_by": user.id,
                "sales_manager": doc.sales_manager or user.id,
                "cashbox": user.cashbox_id,
                "is_deleted": False,
                "priority": doc.priority,
                "is_marketplace_order": doc.is_marketplace_order
            }
            docs_rows.append(doc_dict)

        settings_ids: list[int | None] = [None] * len(settings_payload)
        bulk_settings = [s for s in settings_payload if s]

        if bulk_settings:
            inserted_settings = await database.fetch_all(
                docs_sales_settings.insert()
                .values(bulk_settings)
                .returning(docs_sales_settings.c.id)
            )

            id_iter = iter(r.id for r in inserted_settings)
            for idx, payload in enumerate(settings_payload):
                if payload is not None:
                    settings_ids[idx] = next(id_iter)

        for pos, sid in enumerate(settings_ids):
            docs_rows[pos]["settings"] = sid

        inserted_docs = await database.fetch_all(
            docs_sales.insert()
            .values(docs_rows)
            .returning(
                docs_sales.c.id, docs_sales.c.organization,
                docs_sales.c.number, docs_sales.c.warehouse
            )
        )

        doc_sum_updates: list[dict[str, Any]] = []

        for created, doc_in in zip(inserted_docs, docs_sales_data.__root__):
            doc_id = created.id
            org_id = created.organization
            wh_id = created.warehouse
            goods = doc_in.goods or []
            paid_r = float(doc_in.paid_rubles or 0)
            paid_lt = float(doc_in.paid_lt or 0)
            card_id = doc_in.loyality_card_id
            tags = doc_in.tags or ""

            full_payment = paid_r + paid_lt

            if tags:
                tags_rows.extend({"docs_sales_id": doc_id, "name": t.strip()} for t in tags.split(",") if t.strip())

            total, cashback_sum = 0.0, 0.0

            for g in goods:
                row = {
                    "docs_sales_id": doc_id,
                    "nomenclature": int(g.nomenclature),
                    "price_type": g.price_type,
                    "price": g.price,
                    "quantity": g.quantity,
                    "unit": g.unit,
                    "tax": g.tax,
                    "discount": g.discount,
                    "sum_discounted": g.sum_discounted,
                    "status": g.status,
                }
                goods_rows.append(row)
                total += g.price * g.quantity

                if wh_id:
                    key = (wh_id, row["nomenclature"], org_id)
                    wb_rows_dict[key] = wb_rows_dict.get(key, 0) + row["quantity"]

                if card_id:
                    share_rubles = paid_r / full_payment if full_payment else 0
                    nom = nomenclature_cache[int(g.nomenclature)]
                    if nom.cashback_type == NomenclatureCashbackType.no_cashback:
                        pass
                    elif nom.cashback_type == NomenclatureCashbackType.percent:
                        current_percent = g.price * g.quantity * (nom.cashback_value / 100)
                        cashback_sum += share_rubles * current_percent
                    elif nom.cashback_type == NomenclatureCashbackType.const:
                        cashback_sum += g.quantity * nom.cashback_value
                    elif nom.cashback_type == NomenclatureCashbackType.lcard_cashback:
                        current_percent = g.price * g.quantity * ((card_info[card_id]["percent"] or 0) / 100)
                        cashback_sum += share_rubles * current_percent
                    else:
                        current_percent = g.price * g.quantity * ((card_info[card_id]["percent"] or 0) / 100)
                        cashback_sum += share_rubles * current_percent

            if paid_r:
                payments_rows.append({
                    "contragent": doc_in.contragent,
                    "type": "incoming",
                    "name": f"Оплата по документу {created.number}",
                    "amount": round(paid_r, 2),
                    "amount_without_tax": round(paid_r, 2),
                    "tax": 0,
                    "tax_type": "internal",
                    "article_id": article_id,
                    "article": "Продажи",
                    "paybox": doc_in.paybox or default_paybox,
                    "date": int(datetime.now().timestamp()),
                    "account": user.user,
                    "cashbox": user.cashbox_id,
                    "status": doc_in.status,
                    "stopped": True,
                    "is_deleted": False,
                    "created_at": int(datetime.now().timestamp()),
                    "updated_at": int(datetime.now().timestamp()),
                    "docs_sales_id": doc_id,
                    "tags": tags,
                })
                e2e_rows.append(("p", doc_id))

            if card_id and paid_lt:
                info = card_info.get(card_id) or {}
                lt_rows.append({
                    "loyality_card_id": card_id,
                    "loyality_card_number": info.get("number"),
                    "type": "withdraw",
                    "name": f"Оплата по документу {created.number}",
                    "amount": paid_lt,
                    "created_by_id": user.id,
                    "card_balance": info.get("balance"),
                    "dated": datetime.utcnow(),
                    "cashbox": user.cashbox_id,
                    "tags": doc_in.tags or "",
                    "status": True,
                    "is_deleted": False,
                })
                e2e_rows.append(("l", doc_id))
                card_withdraw_total[card_id] += paid_lt

            if card_id and cashback_sum > 0:
                lt_rows.append(
                    {
                        "loyality_card_id": card_id,
                        "loyality_card_number": card_info[card_id]["number"],
                        "type": "accrual",
                        "name": f"Кэшбэк по документу {created.number}",
                        "amount": round(cashback_sum, 2),
                        "created_by_id": user.id,
                        "card_balance": card_info[card_id]["balance"],
                        "dated": datetime.utcnow(),
                        "cashbox": user.cashbox_id,
                        "tags": tags,
                        "status": True,
                        "is_deleted": False,
                    }
                )
                e2e_rows.append(("l", doc_id))
                card_accrual_total[card_id] += round(cashback_sum, 2)

            if generate_out and wh_id:
                out_docs.append((
                    {
                        "number": None,
                        "dated": doc_in.dated,
                        "docs_sales_id": doc_id,
                        "warehouse": wh_id,
                        "contragent": doc_in.contragent,
                        "organization": org_id,
                        "operation": "outgoing",
                        "status": True,
                        "comment": doc_in.comment,
                    },
                    [
                        {
                            "price_type": 1,
                            "price": 0,
                            "quantity": g.quantity,
                            "unit": g.unit,
                            "nomenclature": int(g.nomenclature)
                        } for g in goods
                    ],
                ))

            doc_sum_updates.append({"id": created.id, "sum": round(total, 2)})

        if doc_sum_updates:
            query_update_sums = """
                UPDATE docs_sales
                SET sum = :sum
                WHERE id = :id
            """
            await database.execute_many(query=query_update_sums, values=doc_sum_updates)

        if goods_rows:
            await database.execute_many(docs_sales_goods.insert(), goods_rows)
        if tags_rows:
            await database.execute_many(docs_sales_tags.insert(), tags_rows)
        payment_ids_map = {}
        if payments_rows:
            inserted_pmt = await database.fetch_all(
                payments.insert()
                .values(payments_rows)
                .returning(payments.c.id)
            )
            payment_ids = [row.id for row in inserted_pmt]
            payment_ids_map = dict(zip([i for i, t in enumerate(e2e_rows) if t[0] == "p"], payment_ids))

        lt_ids_map = {}
        if lt_rows:
            inserted_lt = await database.fetch_all(
                loyality_transactions.insert()
                .values([{k: v for k, v in row.items() if k != "__tmp_id"} for row in lt_rows])
                .returning(loyality_transactions.c.id)
            )
            lt_ids_map = dict(zip(
                [i for i, t in enumerate(e2e_rows) if t[0] == "l"],
                [r.id for r in inserted_lt],
            ))

            # if card_withdraw_total:
            #     query_update_cards = """
            #         UPDATE loyality_cards
            #         SET balance = balance - :amount
            #         WHERE id = :id
            #     """
            #     await database.execute_many(
            #         query=query_update_cards,
            #         values=[{"id": cid, "amount": amt} for cid, amt in card_withdraw_total.items()],
            #     )

        if e2e_rows:
            e2e_to_insert = []
            for idx, (typ, doc_id) in enumerate(e2e_rows):
                to_id = payment_ids_map.get(idx) if typ == "p" else lt_ids_map.get(idx)
                e2e_to_insert.append({
                    "from_entity": 7,
                    "to_entity": 5 if typ == "p" else 6,
                    "from_id": doc_id,
                    "to_id": to_id,
                    "cashbox_id": user.cashbox_id,
                    "type": "docs_sales_payments" if typ == "p" else "docs_sales_loyality_transactions",
                    "status": True,
                    "delinked": False,
                })
            query = (
                entity_to_entity.insert()
                .values(e2e_to_insert)
            )
            await database.execute(query)

        if wb_rows_dict:
            conditions = [
                and_(
                    warehouse_balances.c.warehouse_id == wh,
                    warehouse_balances.c.nomenclature_id == nom,
                )
                for wh, nom, _ in wb_rows_dict.keys()
            ]

            subq = (
                select(
                    warehouse_balances.c.warehouse_id,
                    warehouse_balances.c.nomenclature_id,
                    warehouse_balances.c.current_amount,
                    func.row_number().over(
                        partition_by=(
                            warehouse_balances.c.warehouse_id,
                            warehouse_balances.c.nomenclature_id,
                        ),
                        order_by=warehouse_balances.c.created_at.desc(),
                    ).label("rn"),
                )
                .where(
                    warehouse_balances.c.cashbox_id == user.cashbox_id,
                    or_(*conditions),
                )
            ).subquery()

            latest = await database.fetch_all(
                select(subq).where(subq.c.rn == 1)
            )
            latest_map = {(r.warehouse_id, r.nomenclature_id): r.current_amount for r in latest}

            wb_to_insert = []
            for (wh, nom, org), qty in wb_rows_dict.items():
                prev = latest_map.get((wh, nom), 0)
                wb_to_insert.append({
                    "organization_id": org,
                    "warehouse_id": wh,
                    "nomenclature_id": nom,
                    "document_sale_id": None,
                    "outgoing_amount": qty,
                    "current_amount": prev - qty,
                    "cashbox_id": user.cashbox_id,
                })

            # Выбираем номенклатуры, которые являются ("product", "property"):
            filtered_wb_to_insert = []
            for record in wb_to_insert:
                nomenclature_id = record["nomenclature_id"]
                nomenclature_db = nomenclature_cache.get(nomenclature_id)
                if nomenclature_db and nomenclature_db.type in ("product", "property"):
                    filtered_wb_to_insert.append(record)
            # Вставляем только те записи, которые соответствуют условиям
            if filtered_wb_to_insert:
                await database.execute_many(warehouse_balances.insert(), filtered_wb_to_insert)

        for payload, goods in out_docs:
            asyncio.create_task(create_warehouse_docs(token, {**payload, "goods": goods}, user.cashbox_id))


        asyncio.create_task(raschet(user, token))
        for card_id in set(list(card_withdraw_total) + list(card_accrual_total)):
            asyncio.create_task(raschet_bonuses(card_id))

        # await rabbitmq_messaging.publish(
        #     RecalculateFinancialsMessageModel(
        #         message_id=uuid.uuid4(),
        #         cashbox_id=user.cashbox_id,
        #         token=token,
        #     ),
        #     routing_key="recalculate.financials"
        # )
        #
        # await rabbitmq_messaging.publish(
        #     RecalculateLoyaltyPointsMessageModel(
        #         message_id=uuid.uuid4(),
        #         loyalty_card_ids=list(set(list(card_withdraw_total) + list(card_accrual_total)))
        #     ),
        #     routing_key="recalculate.loyalty_points"
        # )

        rows = await database.fetch_all(
            select(docs_sales).where(docs_sales.c.id.in_([r.id for r in inserted_docs]))
        )
        result = [datetime_to_timestamp(r) for r in rows]
        asyncio.create_task(
            manager.send_message(token, {"action": "create", "target": "docs_sales", "result": result})
        )

        # Юкасса

        # yookassa_oauth_service = OauthService(
        #     oauth_repository = YookassaOauthRepository(),
        #     request_repository = YookassaRequestRepository(),
        #     get_oauth_credential_function = GetOauthCredentialFunction()
        # )
        #
        # yookassa_api_service = YookassaApiService(
        #     request_repository = YookassaRequestRepository(),
        #     oauth_repository = YookassaOauthRepository(),
        #     payments_repository = YookassaPaymentsRepository(),
        #     crm_payments_repository = YookassaCrmPaymentsRepository(),
        #     table_nomenclature_repository = YookassaTableNomenclature(),
        #     amo_table_crm_repository = YookasssaAmoTableCrmRepository(),
        # )
        # payments_ids = await database.fetch_all(
        #     select(
        #         payments.c.id
        #     ).where(
        #         payments.c.docs_sales_id.in_([doc["id"] for doc in inserted_docs])
        #     )
        # )
        #
        # contragents_data = await database.fetch_all(
        #     select(
        #         contragents.c.id,
        #         contragents.c.name,
        #         contragents.c.phone,
        #         contragents.c.email
        #     ).where(
        #         contragents.c.id.in_([doc.contragent for doc in docs_sales_data.__root__])
        #     )
        # )
        #
        # payment_subject = {
        #     "product":"commodity",
        #     "service":"service"
        # }
        #
        # for created, data, payment_id, contragent_data in zip(inserted_docs, docs_sales_data.__root__, payments_ids, contragents_data):
        #     if await yookassa_oauth_service.validation_oauth(user.cashbox_id, data.warehouse):
        #         payment_items_data = []
        #
        #         goods_sum_item = sum([good.price*good.quantity for good in data.goods])
        #
        #         discount_sum_item = round(abs(data.paid_rubles - goods_sum_item), 2)
        #
        #         for index, good in enumerate(data.goods):
        #             payment_items_data.append(
        #                 ItemModel(
        #                             description = (await database.fetch_one(select(nomenclature.c.name).where(nomenclature.c.id == int(good.nomenclature)))).name or "Товар",
        #                             amount = AmountModel(
        #                                 value = str(round((good.price*good.quantity - discount_sum_item*(good.price*good.quantity/goods_sum_item))/good.quantity, 2)),
        #                                 currency = "RUB"
        #                             ),
        #                             payment_mode = "full_payment",
        #                             payment_subject = payment_subject.get((await database.fetch_one(select(nomenclature.c.type).where(nomenclature.c.id == int(good.nomenclature)))).type),
        #                             quantity = good.quantity,
        #                             vat_code = "1"
        #                         ))
        #         sum_goods_diff = [decimal.Decimal(item.amount.value)*int(item.quantity) for item in payment_items_data]
        #
        #         if round(sum(sum_goods_diff), 2):
        #             await yookassa_api_service.api_create_payment(
        #                 user.cashbox_id,
        #                 data.warehouse,
        #                 created["id"],
        #                 payment_id.get("id"),
        #                 PaymentCreateModel(
        #                     amount = AmountModel(
        #                         value = str(round(sum(sum_goods_diff), 2)),
        #                         currency = "RUB"
        #                     ),
        #                     description = f"Оплата по документу {created['number']}",
        #                     capture = True,
        #                     receipt = ReceiptModel(
        #                         customer = CustomerModel(
        #                             full_name = contragent_data.name,
        #                             email = contragent_data.email,
        #                             phone = f'{phonenumbers.parse(contragent_data.phone,"RU").country_code}{phonenumbers.parse(contragent_data.phone,"RU").national_number}',
        #                         ),
        #                         items = payment_items_data,
        #                     ),
        #                     confirmation = ConfirmationRedirect(
        #                         type = "redirect",
        #                         return_url = f"https://${os.getenv('APP_URL')}/?token=${token}"
        #                     )
        #                 )
        #             )

        for create in docs_sales_data.__root__:
            if create.tech_card_operation_uuid:
                await rabbitmq_messaging.publish(
                    TechCardWarehouseOperationMessage(
                        message_id=uuid.uuid4(),
                        tech_card_operation_uuid=create.tech_card_operation_uuid,
                        user_cashbox_id=user.cashbox_id,
                    ),
                    routing_key="teach_card_operation"
                )
        return result

    async def _validate_fk(self, table, ids: Set[int], name: str):
        if not ids:
            return
        rows = await database.fetch_all(select(table.c.id).where(table.c.id.in_(ids)))
        found = {r.id for r in rows}
        missing = ids - found
        if missing:
            raise HTTPException(
                400,
                detail=f"{name}.id: {', '.join(map(str, sorted(missing)))} не найден(ы)"
            )
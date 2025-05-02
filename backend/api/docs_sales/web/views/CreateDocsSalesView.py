import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Set, List, Tuple, Any

from fastapi import HTTPException
from sqlalchemy import select, and_, or_, desc, tuple_, func, bindparam

from api.docs_sales import schemas
from api.docs_warehouses.utils import create_warehouse_docs
from api.loyality_transactions.routers import raschet_bonuses
from database.db import database, contragents, contracts, organizations, warehouses, users_cboxes_relation, \
    nomenclature, price_types, units, articles, pboxes, fifo_settings, docs_sales, loyality_cards, docs_sales_goods, \
    docs_sales_tags, payments, loyality_transactions, entity_to_entity, warehouse_balances, docs_sales_settings
from functions.helpers import get_user_by_token, datetime_to_timestamp
from functions.users import raschet
from ws_manager import manager


class CreateDocsSalesView:

    async def __call__(
        self,
        token: str,
        docs_sales_data: schemas.CreateMass,
        generate_out: bool = True
    ):
        user = await get_user_by_token(token)

        fks = defaultdict(set)
        org_date_pairs: List[Tuple[int, int]] = []
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

        if org_date_pairs:
            conds = [and_(fifo_settings.c.organization_id == org, fifo_settings.c.blocked_date >= date)
                     for org, date in org_date_pairs]
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
                ).where(loyality_cards.c.id.in_(fks["loyality_cards"]))
            )
            card_info = {r.id: {"number": r.card_number, "balance": r.balance} for r in rows}

        card_withdraw_total: dict[int, float] = defaultdict(float)

        docs_rows, goods_rows, tags_rows = [], [], []
        payments_rows, lt_rows, e2e_rows = [], [], []
        wb_rows_dict: dict[tuple[int, int, int], float] = {}
        out_docs: list[tuple[dict, list[dict]]] = []

        settings_payload: list[dict | None] = []

        for idx, doc in enumerate(docs_sales_data.__root__):
            settings_payload.append(doc.settings.dict(exclude_unset=True) if doc.settings else None)

            number = doc.number
            if not number:
                last = await database.fetch_val(
                    select(docs_sales.c.number)
                    .where(
                        docs_sales.c.organization == doc.organization,
                        docs_sales.c.is_deleted.is_(False),
                    )
                    .order_by(desc(docs_sales.c.id)).limit(1), column=0,
                )
                try:
                    number = str(int(last) + 1) if last else "1"
                except Exception:
                    number = "1"

            doc_dict = {
                "number": number, "dated": doc.dated,
                "sum": 0.0,
                "contragent": doc.contragent,
                "contract": doc.contract,
                "organization": doc.organization,
                "warehouse": doc.warehouse,
                "comment": doc.comment,
                "tags": doc.tags,
                "status": doc.status,
                "created_by": user.id,
                "sales_manager": doc.sales_manager or user.id,
                "cashbox": user.cashbox_id,
                "is_deleted": False
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
                if payload:
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

            if tags:
                tags_rows.extend({"docs_sales_id": doc_id, "name": t.strip()} for t in tags.split(",") if t.strip())

            total = 0.0

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

            if card_withdraw_total:
                query_update_cards = """
                    UPDATE loyality_cards
                    SET balance = balance - :amount
                    WHERE id = :id
                """
                await database.execute_many(
                    query=query_update_cards,
                    values=[{"id": cid, "amount": amt} for cid, amt in card_withdraw_total.items()],
                )

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
            await database.execute_many(entity_to_entity.insert(), e2e_to_insert)

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
            await database.execute_many(warehouse_balances.insert(), wb_to_insert)

        for payload, goods in out_docs:
            asyncio.create_task(create_warehouse_docs(token, {**payload, "goods": goods}, user.cashbox_id))

        asyncio.create_task(raschet(user, token))

        for card_id in card_withdraw_total:
            asyncio.create_task(raschet_bonuses(card_id))

        rows = await database.fetch_all(
            select(docs_sales).where(docs_sales.c.id.in_([r.id for r in inserted_docs]))
        )
        result = [datetime_to_timestamp(r) for r in rows]
        asyncio.create_task(
            manager.send_message(token, {"action": "create", "target": "docs_sales", "result": result})
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
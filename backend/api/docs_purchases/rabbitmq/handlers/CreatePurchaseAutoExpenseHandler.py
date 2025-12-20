from sqlalchemy import text

from api.docs_warehouses.utils import create_warehouse_docs
from database.db import database, docs_purchases, docs_purchases_goods, nomenclature


class CreatePurchaseAutoExpenseHandler:
    async def __call__(self, event=None, message=None, *args, **kwargs):
        payload = None
        for c in (event, message, *args, kwargs.get("payload"), kwargs.get("message")):
            if isinstance(c, dict):
                payload = c
                break

        if not payload:
            print(
                f"[purchase-auto-expense] bad payload types: event={type(event)} message={type(message)}"
            )
            return

        purchase_id = payload.get("purchase_id") or payload.get("purchaseId")
        cashbox_id = (
            payload.get("cashbox_id")
            or payload.get("cashboxId")
            or payload.get("cashbox")
        )
        token = payload.get("token")

        if purchase_id is None or cashbox_id is None:
            print(f"[purchase-auto-expense] bad payload: {payload}")
            return

        await self.handle(int(purchase_id), int(cashbox_id), token)

    async def handle(self, purchase_id: int, cashbox_id: int, token=None):
        purchase = await database.fetch_one(
            docs_purchases.select().where(
                docs_purchases.c.id == purchase_id,
                docs_purchases.c.cashbox == cashbox_id,
                docs_purchases.c.is_deleted.is_not(True),
            )
        )
        if not purchase:
            print(f"[purchase-auto-expense] purchase not found: {purchase_id}")
            return

        goods_rows = await database.fetch_all(
            docs_purchases_goods.select().where(
                docs_purchases_goods.c.docs_purchases_id == purchase_id
            )
        )
        if not goods_rows:
            print(f"[purchase-auto-expense] no goods for purchase: {purchase_id}")
            return

        goods_res = []
        for g in goods_rows:
            nom_id = int(g["nomenclature"])
            nom = await database.fetch_one(
                nomenclature.select().where(nomenclature.c.id == nom_id)
            )
            if not nom:
                continue

            try:
                nom_type = nom["type"]
            except Exception:
                nom_type = getattr(nom, "type", None)

            if nom_type != "product":
                continue

            unit_id = g["unit"] or (
                nom["unit"] if "unit" in nom else getattr(nom, "unit", None)
            )
            goods_res.append(
                {
                    "price_type": 1,
                    "price": 0,
                    "quantity": g["quantity"],
                    "unit": unit_id,
                    "nomenclature": nom_id,
                }
            )

        if not goods_res:
            print(
                f"[purchase-auto-expense] only services/empty goods for purchase: {purchase_id}"
            )
            return

        if not token:
            row = await database.fetch_one(
                text("select token from relation_tg_cashboxes where id = :id"),
                {"id": cashbox_id},
            )
            token = row["token"] if row and row.get("token") else None
        if not token:
            print(
                f"[purchase-auto-expense] token not found for cashbox_id={cashbox_id}, purchase={purchase_id}"
            )
            return

        body = {
            "number": None,
            "dated": purchase["dated"],
            "docs_purchases": purchase_id,
            "docs_sales_id": None,
            "to_warehouse": None,
            "organization": purchase["organization"],
            "status": False,
            "contragent": purchase["contragent"],
            "operation": "outgoing",
            "comment": purchase["comment"],
            "warehouse": purchase["warehouse"],
            "goods": goods_res,
        }

        await create_warehouse_docs(token, body, cashbox_id)
        print(f"[purchase-auto-expense] OK purchase={purchase_id}")

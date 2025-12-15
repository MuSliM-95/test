from api.docs_purchases.rabbitmq.messages.CreatePurchaseAutoExpenseMessage import (
    CreatePurchaseAutoExpenseMessage,
)
from api.docs_warehouses.utils import create_warehouse_docs
from database.db import (
    database,
    docs_purchases,
    docs_purchases_goods,
    nomenclature,
)


class CreatePurchaseAutoExpenseHandler:
    async def handle(self, message: CreatePurchaseAutoExpenseMessage) -> None:
        # 1) достаём закупку
        purchase = await database.fetch_one(
            docs_purchases.select().where(
                docs_purchases.c.id == message.purchase_id,
                docs_purchases.c.cashbox == message.cashbox_id,
                docs_purchases.c.is_deleted.is_not(True),
            )
        )
        if not purchase:
            print(f"[purchase-auto-expense] purchase not found: {message.purchase_id}")
            return

        # 2) достаём товары закупки
        goods_rows = await database.fetch_all(
            docs_purchases_goods.select().where(
                docs_purchases_goods.c.docs_purchases_id == message.purchase_id
            )
        )
        if not goods_rows:
            print(
                f"[purchase-auto-expense] no goods for purchase: {message.purchase_id}"
            )
            return

        # 3) берём только продукты (services исключаем)
        goods_res = []
        for g in goods_rows:
            nom_id = int(g["nomenclature"])
            nom = await database.fetch_one(
                nomenclature.select().where(nomenclature.c.id == nom_id)
            )
            if not nom or nom.type != "product":
                continue

            unit_id = g["unit"] or getattr(nom, "unit", None)
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
                f"[purchase-auto-expense] only services/empty goods for purchase: {message.purchase_id}"
            )
            return

        # 4) создаём расходный документ склада (operation = outgoing)
        body = {
            "number": None,
            "dated": purchase["dated"],
            "docs_purchases": message.purchase_id,  # привязка к закупке
            "docs_sales_id": None,  # чтобы check_relationship не падал
            "to_warehouse": None,
            "organization": purchase["organization"],
            "status": False,  # как договорено: создаём черновик, без движения
            "contragent": purchase["contragent"],
            "operation": "outgoing",
            "comment": purchase["comment"],
            "warehouse": purchase["warehouse"],
            "goods": goods_res,
        }

        await create_warehouse_docs(message.token, body, message.cashbox_id)
        print(f"[purchase-auto-expense] OK purchase={message.purchase_id}")

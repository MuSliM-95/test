from typing import Optional

from sqlalchemy import and_, asc, select

from api.docs_warehouses.func_warehouse import call_type_movement
from database.db import database, docs_warehouse
from functions.helpers import datetime_to_timestamp
from ws_manager import manager


async def _find_existing_warehouse_doc_id(doc: dict, cashbox_id: int) -> Optional[int]:
    operation = doc.get("operation")
    if not operation:
        return None

    purchase_id = doc.get("docs_purchases")
    sales_id = doc.get("docs_sales_id")

    # Идемпотентность только если есть привязка к закупке/продаже
    if purchase_id is None and sales_id is None:
        return None

    filters = [
        docs_warehouse.c.cashbox == cashbox_id,
        docs_warehouse.c.is_deleted.is_not(True),
        docs_warehouse.c.operation == operation,
    ]

    if purchase_id is not None:
        filters.append(docs_warehouse.c.docs_purchases == int(purchase_id))
    if sales_id is not None:
        filters.append(docs_warehouse.c.docs_sales_id == int(sales_id))

    row = await database.fetch_one(select(docs_warehouse.c.id).where(and_(*filters)))
    return int(row["id"]) if row else None


async def create_warehouse_docs(token: str, doc, cashbox_id: int):
    """
    Идемпотентное создание/обновление складского документа.
    Если уже есть docs_warehouse для (operation + docs_purchases/docs_sales_id) — обновляем его,
    а не создаём новый. Это предотвращает дубли при ретраях/повторных вызовах.
    """
    if doc is None:
        return []

    if not isinstance(doc, dict):
        # Раньше аннотация была list, но по факту везде передаётся dict.
        raise TypeError(f"create_warehouse_docs: expected dict, got {type(doc)}")

    # Если нашли существующий документ — подставляем id, чтобы call_type_movement пошёл по update
    existing_id = await _find_existing_warehouse_doc_id(doc, cashbox_id)
    if existing_id:
        doc["id"] = existing_id

        # Не затираем number в NULL (в body часто number=None)
        if doc.get("number") is None:
            doc.pop("number", None)

    response = await call_type_movement(
        doc["operation"], entity_values=doc, token=token
    )

    query = docs_warehouse.select().where(docs_warehouse.c.id == response)
    docs_warehouse_db = await database.fetch_all(query)
    docs_warehouse_db = [*map(datetime_to_timestamp, docs_warehouse_db)]

    # перенумерация как было раньше (не трогаем логику, чтобы не ломать UI/ожидания)
    q = (
        docs_warehouse.select()
        .where(
            docs_warehouse.c.cashbox == cashbox_id, docs_warehouse.c.is_deleted == False
        )
        .order_by(asc(docs_warehouse.c.id))
    )
    docs_db = await database.fetch_all(q)

    for i in range(0, len(docs_db)):
        if not docs_db[i].number:
            q = (
                docs_warehouse.update()
                .where(docs_warehouse.c.id == docs_db[i].id)
                .values({"number": str(i + 1)})
            )
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

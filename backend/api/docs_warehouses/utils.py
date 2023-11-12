from api.docs_warehouses.func_warehouse import call_type_movement
from database.db import (
    database,
    docs_warehouse,
    docs_warehouse_goods,
    organizations,
    price_types,
    warehouse_balances,
    warehouse_register_movement,
    warehouses,
)

from functions.helpers import (
    check_entity_exists,
    check_period_blocked,
    check_unit_exists,
    datetime_to_timestamp,
    get_user_by_token,
)
from sqlalchemy import asc

from ws_manager import manager

async def create_warehouse_docs(token: str, doc: list, cashbox_id: int):

    response = await call_type_movement(doc['operation'], entity_values=doc, token=token)

    query = docs_warehouse.select().where(docs_warehouse.c.id == response)
    docs_warehouse_db = await database.fetch_all(query)
    docs_warehouse_db = [*map(datetime_to_timestamp, docs_warehouse_db)]

    q = docs_warehouse.select().where(
            docs_warehouse.c.cashbox == cashbox_id,
            docs_warehouse.c.is_deleted == False
        ).order_by(asc(docs_warehouse.c.id))

    docs_db = await database.fetch_all(q)

    for i in range(0, len(docs_db)):
        if not docs_db[i].number:
            q = docs_warehouse.update().where(docs_warehouse.c.id == docs_db[i].id).values({ "number": str(i + 1) })
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
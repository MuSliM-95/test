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

from ws_manager import manager

async def create_warehouse_docs(token: str, doc: list):

    response = await call_type_movement(doc['operation'], entity_values=doc, token=token)

    query = docs_warehouse.select().where(docs_warehouse.c.id == response)
    docs_warehouse_db = await database.fetch_all(query)
    docs_warehouse_db = [*map(datetime_to_timestamp, docs_warehouse_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "docs_warehouse",
            "result": docs_warehouse_db,
        },
    )

    return docs_warehouse_db
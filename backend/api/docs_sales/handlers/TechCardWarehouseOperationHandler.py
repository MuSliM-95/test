import dataclasses
from typing import Mapping, Any, Optional

from aio_pika import IncomingMessage

from api.docs_sales.messages.TechCardWarehouseOperationMessage import TechCardWarehouseOperationMessage
from common.amqp_messaging.common.core.EventHandler import IEventHandler

from api.tech_operations.models import TechOperationDB, TechOperationComponentDB
from api.tech_cards.models import TechCardDB

from sqlalchemy import select

from functions.helpers import get_user_by_token

from database.db import warehouse_balances, database
class TechCardWarehouseOperationHandler(IEventHandler[TechCardWarehouseOperationMessage]):
    async def __call__(self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None):
        user = await get_user_by_token(event["token"])
        async with database.transaction():
            query = (
                select(TechOperationDB, TechCardDB, TechOperationComponentDB)
                .join(TechCardDB, TechOperationDB.tech_card_id == TechCardDB.id)
                .where(TechOperationDB.id == event["tech_card_operation_uuid"])
            )
            row = await database.fetch_one(query)
            tech_card_type = row["card_type"]
            # 1 режим - списание сырья со склада
            if tech_card_type == 'automatic':
                query_tech_card_operation_components = (
                    select(TechOperationComponentDB)
                    .where(TechOperationComponentDB.operation_id == event["tech_card_operation_uuid"])
                )
                tech_card_operation_db = await database.fetch_all(query_tech_card_operation_components)
                for component in tech_card_operation_db:
                    query_warehouse_balance = warehouse_balances.select().where(
                        warehouse_balances.c.cashbox_id == user.cashbox_id,
                        warehouse_balances.c.nomenclature_id == component.nomeclature_id).order_by(warehouse_balances.c.id.desc())
                    warehouse_balances_db = await database.fetch_one(query_warehouse_balance)
                    current_amount = warehouse_balances_db.current_amount if warehouse_balances_db else 0
                    query_insert_warehouse_balance = warehouse_balances.insert().values(
                        cashbox_id=user.cashbox_id,
                        warehouse_id=row['from_warehouse_id'],
                        nomenclature_id=component.nomeclature_id,
                        outgoing_amount=component.quantity,
                        current_amount=current_amount - component.quantity,
                    )
                    await database.execute(query_insert_warehouse_balance)

            # 2 режим - списание сырья со склада + приход готовой продукции на склад
            elif tech_card_type == "reference":
                query_tech_card_operation_components = (
                    select(TechOperationComponentDB)
                    .where(TechOperationComponentDB.operation_id == event["tech_card_operation_uuid"])
                )
                tech_card_operation_db = await database.fetch_all(query_tech_card_operation_components)
                for component in tech_card_operation_db:
                    query_warehouse_balance = warehouse_balances.select().where(
                        warehouse_balances.c.cashbox_id == user.cashbox_id,
                        warehouse_balances.c.nomenclature_id == component.nomeclature_id).order_by(warehouse_balances.c.id.desc())
                    
                    warehouse_balances_db = await database.fetch_one(query_warehouse_balance)
                    current_amount = warehouse_balances_db.current_amount if warehouse_balances_db else 0
                    query_insert_warehouse_balance = warehouse_balances.insert().values(
                        cashbox_id=user.cashbox_id,
                        warehouse_id=row['from_warehouse_id'],
                        nomenclature_id=component.nomeclature_id,
                        outgoing_amount=component.quantity,
                        current_amount=current_amount - component.quantity,
                    )
                    await database.execute(query_insert_warehouse_balance)
                
                # Приход готовой продукции на склад
                query_current_amount = warehouse_balances.select().where(
                    warehouse_balances.c.cashbox_id == user.cashbox_id,
                    warehouse_balances.c.nomenclature_id == row['nomenclature_id']
                ).order_by(warehouse_balances.c.id.desc())
                warehouse_balances_db = await database.fetch_one(query_current_amount)
                current_amount = warehouse_balances_db.current_amount if warehouse_balances_db else 0
                incloming_produced_product_query = warehouse_balances.insert().values(
                    cashbox_id=user.cashbox_id,
                    warehouse_id=row['to_warehouse_id'],
                    nomenclature_id=row['nomenclature_id'],
                    incoming_amount=row['output_quantity'],
                    current_amount=current_amount + row['output_quantity'],
                )
                await database.execute(incloming_produced_product_query)

            else:
                raise ValueError("Unknown tech card type")

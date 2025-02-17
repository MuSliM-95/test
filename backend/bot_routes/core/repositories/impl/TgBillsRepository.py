from typing import Union
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from database.db import tg_bot_bills, tochka_bank_accounts, database

from bot_routes.core.repositories.core.ITgBillsRepository import ITgBillsRepository
from bot_routes.core.models.ITgBills import (
    ITgBillsUpdate,
    ITgBillsCreate,
    ITgBillsExtended,
)


class TgBillsRepository(ITgBillsRepository):

    async def update(self, id, bill: ITgBillsUpdate) -> None:
        try:
            query = (
                tg_bot_bills.update()
                .where(tg_bot_bills.c.id == id)
                .values(**bill.dict(exclude_unset=True))
            )
            await database.execute(query)
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}") 

    async def insert(self, bill: ITgBillsCreate) -> int:
        try:
            query = tg_bot_bills.insert().values(**bill.dict())
            result = await database.execute(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")


    async def delete(self, id: int) -> None:
        try:
            query = tg_bot_bills.delete().where(tg_bot_bills.c.id == id)
            await database.execute(query)
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def get_by_id(self, id: int) -> Union[ITgBillsExtended, None]:
        try:
            query = (
                select([tg_bot_bills, tochka_bank_accounts.c.accountId])
                .select_from(tg_bot_bills)
                .outerjoin(tochka_bank_accounts, tg_bot_bills.c.tochka_bank_account_id == tochka_bank_accounts.c.id)
                .where(tg_bot_bills.c.id == id)
            )
            result = await database.fetch_one(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")


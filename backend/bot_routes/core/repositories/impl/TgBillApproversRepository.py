from typing import List, Union
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from bot_routes.core.repositories.core.ITgBillApproversRepository import ITgBillApproversRepository
from bot_routes.core.models.ITgBillApprovers import (
    ITgBillApproversCreate,
    ITgBillApproversUpdate,
    ITgBillApprovers,
    ITgBillApproversExtended,
)


class TgBillApproversRepository(ITgBillApproversRepository):
    def __init__(self, database, bill_approvers, users):
        self.database = database
        self.users = users
        self.bill_approvers = bill_approvers

    async def update(self, id: int, bill: ITgBillApproversUpdate) -> None:
        try:
            query = (
                self.bill_approvers
                .update()
                .where(self.bill_approvers.c.id == id)
                .values(**bill.dict(exclude_unset=True))
            )
            await self.database.execute(query)
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def insert(self, bill: ITgBillApproversCreate) -> int:
        try:
            query = self.bill_approvers.insert().values(**bill.dict())
            result = await self.database.execute(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")


    async def delete(self, id: int) -> None:
        try:
            query = self.bill_approvers.delete().where(self.bill_approvers.c.id == id)
            await self.database.execute(query)
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def get_by_id(self, id: int) -> Union[ITgBillApprovers, None]:
        try:
            query = self.bill_approvers.select().where(self.bill_approvers.c.id == id)
            result = await self.database.fetch_one(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def get_approve_by_bill_id_and_approver_id(self, bill_id: int, approver_id: int) -> Union[ITgBillApprovers, None]:
        try:
            query = self.bill_approvers.select().where(
                self.bill_approvers.c.bill_id == bill_id,
                self.bill_approvers.c.approver_id == approver_id
            )
            result = await self.database.fetch_one(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def get_approvers_by_bill_id(self, bill_id: int) -> List[ITgBillApprovers]:
        try:
            query = self.bill_approvers.select().where(self.bill_approvers.c.bill_id == bill_id)
            result = await self.database.fetch_all(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def get_approvers_extended_by_bill_id(self, bill_id: int) -> List[Union[ITgBillApproversExtended, None]]:
        try:
            query = (
                select([self.bill_approvers, self.users.c.username])
                .select_from(self.bill_approvers)
                .join(self.users, self.bill_approvers.c.approver_id == self.users.c.id)
                .where(self.bill_approvers.c.id == bill_id)
            )
            result = await self.database.fetch_all(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")


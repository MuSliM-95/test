from typing import List, Union
from sqlalchemy import select



from bot_routes.core.models.ITgBillApprovers import (
    ITgBillApproversCreate,
    ITgBillApproversUpdate,
    ITgBillApprovers,
    ITgBillApproversExtended
)


class ITgBillApproversRepository:

    def __init__(self, database, bill_approvers, bills, tochka_bank_accounts):
        self.database = database
        self.bill_approvers = bill_approvers
        self.bills = bills
        self.tochka_bank_accounts = tochka_bank_accounts

    async def update(self, id: int, bill: ITgBillApproversUpdate) -> None:
        raise NotImplementedError

    async def insert(self, bill: ITgBillApproversCreate) -> None:
        raise NotImplementedError

    async def delete(self, id: int) -> None:
        raise NotImplementedError

    async def get_by_id(self, id: int) -> Union[ITgBillApprovers, None]:
        raise NotImplementedError

    async def get_approve_by_bill_id_and_approver_id(self, bill_id: int, approve_id: int) -> Union[ITgBillApprovers, None]:
        raise NotImplementedError
    
    async def get_approvers_by_bill_id(self, bill_id: int) -> List[ITgBillApprovers]:
        raise NotImplementedError
    
    async def get_approvers_extended_by_bill_id(self, bill_id: int) -> List[ITgBillApproversExtended]:
        raise NotImplementedError


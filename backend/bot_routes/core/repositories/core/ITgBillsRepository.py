from typing import List, Union

from bot_routes.core.models.ITgBills import (
    ITgBillsUpdate, 
    ITgBillsCreate, 
    ITgBillsExtended
)


class ITgBillsRepository:
    async def update(self, id: int, bill: ITgBillsUpdate) -> None:
        raise NotImplementedError

    async def insert(self, bill: ITgBillsCreate) -> int:  # Return the inserted ID
        raise NotImplementedError

    async def delete(self, id: int) -> None:
        raise NotImplementedError

    async def get_by_id(self, id: int) -> Union[ITgBillsExtended, None]:
        raise NotImplementedError
    


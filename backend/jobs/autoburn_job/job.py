import asyncio
from datetime import datetime, timedelta
from typing import List, Union, Any, Dict
from databases.backends.postgres import Record
from sqlalchemy import select, asc
from database.db import database, loyality_transactions, loyality_cards


class AutoBurn:
    def __init__(self, card: Record) -> None:
        self.card: Record = card
        self.card_balance: float = card.balance
        self.first_operation_burned: Union[int, None] = None
        self.accrual_list: List[dict] = []
        self.withdraw_list: List[dict] = []
        self.burned_list: List[int] = []
        self.autoburn_operation_list: List[dict] = []

    @staticmethod
    async def get_cards() -> List[Record]:
        cards_query = (
            loyality_cards
            .select()
            .where(
                loyality_cards.c.balance > 0,
                loyality_cards.c.lifetime.is_not(None),
                loyality_cards.c.lifetime > 0
            )
        )
        return await database.fetch_all(cards_query)

    async def _get_first_operation_burned(self) -> None:
        q_first = (
            select(loyality_transactions.c.id)
            .where(
                loyality_transactions.c.loyality_card_id == self.card.id,
                loyality_transactions.c.type == "accrual",
                loyality_transactions.c.amount > 0,
                loyality_transactions.c.autoburned.is_not(True),
                loyality_transactions.c.created_at + timedelta(seconds=self.card.lifetime) < datetime.utcnow(),
                loyality_transactions.c.card_balance == 0
            )
            .order_by(asc(loyality_transactions.c.id))
            .limit(1)
        )
        self.first_operation_burned = await database.fetch_val(q_first)

    async def _get_transaction(self) -> None:
        if self.first_operation_burned is not None:
            q = (
                loyality_transactions
                .select()
                .where(
                    loyality_transactions.c.loyality_card_id == self.card.id,
                    loyality_transactions.c.type.in_(["accrual", "withdraw"]),
                    loyality_transactions.c.amount > 0,
                    loyality_transactions.c.autoburned.is_not(True),
                    loyality_transactions.c.id >= self.first_operation_burned
                )
            )
            transaction_list = await database.fetch_all(q)

            minus_index = 0
            self.accrual_list.extend(
                [dict(i, start_amount=i.amount) for i in transaction_list if i.type == "accrual"]
            )
            for transaction in transaction_list:
                transaction: Dict[str, Any] = dict(transaction, start_amount=transaction["amount"])
                self.burned_list.append(transaction["id"])
                if transaction["type"] == "withdraw":
                    if self.accrual_list[minus_index]["amount"] > 0:
                        if self.accrual_list[minus_index]["amount"] >= transaction["amount"]:
                            self.accrual_list[minus_index]["amount"] -= transaction["amount"]
                        else:
                            transaction["amount"] = transaction["amount"] - self.accrual_list[minus_index]["amount"]
                            self.accrual_list[minus_index]["amount"] = 0
                        if self.accrual_list[minus_index]["amount"] == 0:
                            minus_index += 1

                    self.withdraw_list.append(transaction)

    @database.transaction()
    async def _burn(self) -> None:
        update_transaction_status_query = (
            loyality_transactions
            .update()
            .where(
                loyality_transactions.c.id.in_(self.burned_list)
            )
            .values({"autoburned": True})
        )
        await database.execute(update_transaction_status_query)

        update_balance_query = (
            loyality_cards
            .update()
            .where(loyality_cards.c.id == self.card.id)
            .values({"balance": self.card_balance})
        )
        await database.execute(update_balance_query)

        create_transcation_query = (
            loyality_transactions
            .insert()
            .values()
        )
        await database.execute_many(query=create_transcation_query, values=self.autoburn_operation_list)

    def _get_autoburned_operation_dict(
            self,
            update_balance_sum: float,
            start_amount: float,
            created_at: datetime
    ) -> dict:
        return {
            "type": "withdraw",
            "amount": update_balance_sum,
            "loyality_card_id": self.card.id,
            "loyality_card_number": self.card.card_number,
            "created_by_id": self.card.created_by_id,
            "cashbox": self.card.cashbox_id,
            "tags": "",
            "name": f"Автосгорание от {created_at.strftime('%d.%m.%Y')} по сумме {start_amount}",
            "description": None,
            "status": True,
            "external_id": None,
            "cashier_name": None,
            "dead_at": None,
            "is_deleted": False,
            "autoburned": True,
            "card_balance": self.card_balance
        }

    async def start(self) -> None:
        await self._get_first_operation_burned()
        await self._get_transaction()
        for a in self.accrual_list:
            amount, update_balance_sum = a["amount"], 0
            if amount == 0:
                continue

            w = 0
            while w < len(self.withdraw_list):
                if amount == 0:
                    break

                if a["amount"] >= self.withdraw_list[w]["amount"]:
                    update_balance_sum += a["amount"] - self.withdraw_list[w]["amount"]
                    del self.withdraw_list[w]
                    w -= 1
                else:
                    update_balance_sum += a["amount"]
                    self.withdraw_list[w]["amount"] -= a["amount"]
                amount -= update_balance_sum
                w += 1

            if update_balance_sum != 0:
                self.card_balance -= update_balance_sum
                self.autoburn_operation_list.append(
                    self._get_autoburned_operation_dict(
                        update_balance_sum=update_balance_sum, start_amount=a["start_amount"],
                        created_at=a["created_at"]
                    )
                )
            else:
                self.card_balance -= a["amount"]
                self.autoburn_operation_list.append(
                    self._get_autoburned_operation_dict(
                        update_balance_sum=a["amount"], start_amount=a["start_amount"], created_at=a["created_at"]
                    )
                )

        await self._burn()

async def autoburn():
    await database.connect()

    card_list = await AutoBurn.get_cards()
    for card in card_list:
        await AutoBurn(card=card).start()

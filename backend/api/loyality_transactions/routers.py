from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.sql.functions import coalesce

from database.db import database, loyality_transactions, loyality_cards
import api.loyality_transactions.schemas as schemas
from typing import Optional, Dict, Any
from sqlalchemy import desc, func, select, case

from functions.helpers import datetime_to_timestamp, get_entity_by_id, get_filters_transactions, \
    get_entity_by_id_cashbox, clear_phone_number

from ws_manager import manager
from functions.helpers import get_user_by_token

from datetime import datetime
import asyncio

router = APIRouter(tags=["loyality_transactions"])


async def raschet_bonuses(cashbox_id: int) -> None:
    q = (
        loyality_cards
        .select()
        .where(
            loyality_cards.c.cashbox_id == cashbox_id,
            loyality_cards.c.status_card.is_(True),
            loyality_cards.c.is_deleted.is_(False)
        )
    )
    all_cards = await database.fetch_all(q)

    for card in all_cards:
        a_q = (
            coalesce(
                func.sum(loyality_transactions.c.amount)
                .filter(loyality_transactions.c.type == "accrual"), 0
            ).label("income")
        )
        w_q = (
            coalesce(
                func.sum(loyality_transactions.c.amount)
                .filter(loyality_transactions.c.type == "withdraw"), 0
            ).label("outcome")
        )
        q = (
            select(
                a_q, w_q, case((a_q > w_q, a_q - w_q), (a_q < w_q, 0), else_=0).label("balance")
            )
            .filter(
                loyality_transactions.c.loyality_card_id == card.id,
                loyality_transactions.c.status.is_(True),
                loyality_transactions.c.is_deleted.is_(False)
            )
        )
        edit_dict: Dict[str, Any] = dict(await database.fetch_one(q))
        await database.execute(loyality_cards.update().where(loyality_cards.c.id == card.id).values(edit_dict))


@router.get("/loyality_transactions/{idx}/", response_model=schemas.LoyalityTransaction)
async def get_loyality_transaction_by_id(token: str, idx: int):
    """Получение транзакции по ID"""
    user = await get_user_by_token(token)
    loyality_transactions_db = await get_entity_by_id(loyality_transactions, idx, user.id)
    loyality_transactions_db = datetime_to_timestamp(loyality_transactions_db)

    return loyality_transactions_db


@router.get("/loyality_transactions/", response_model=schemas.CountRes)
async def get_transactions(token: str, limit: int = 100, offset: int = 0,
                           filters_q: schemas.LoyalityTranstactionFilters = Depends()):
    """Получение списка транзакций"""
    user = await get_user_by_token(token)
    filters = get_filters_transactions(loyality_transactions, filters_q)
    query = (
        loyality_transactions.select()
        .where(
            loyality_transactions.c.cashbox == user.cashbox_id,
            loyality_transactions.c.is_deleted.is_not(True)
        )
        .order_by(desc(loyality_transactions.c.id))
    )
    count_query = (
        select(func.count(loyality_transactions.c.id))
        .where(
            loyality_transactions.c.cashbox == user.cashbox_id,
            loyality_transactions.c.is_deleted.is_not(True)
        )
    )

    if filters:
        query = query.filter(*filters)
        count_query = count_query.filter(*filters)

    query = query.limit(limit).offset(offset)
    loyality_transactions_db = await database.fetch_all(query)
    loyality_transactions_db = [*map(datetime_to_timestamp, loyality_transactions_db)]

    count = await database.execute(count_query)

    return {"result": loyality_transactions_db, "count": count}


@router.post("/loyality_transactions/", response_model=Optional[schemas.LoyalityTransaction])
async def create_loyality_transaction(token: str, loyality_transaction_data: schemas.LoyalityTransactionCreate):
    """Создание транзакций"""
    user = await get_user_by_token(token)
    loyality_transactions_values = loyality_transaction_data.dict()

    if user:
        if user.status:
            loyality_card_number = clear_phone_number(
                phone_number=loyality_transaction_data.loyality_card_number
            )
            q = loyality_cards.select().where(loyality_cards.c.card_number == loyality_card_number,
                                              loyality_cards.c.cashbox_id == user.cashbox_id)
            card = await database.fetch_one(q)
            if not card:
                raise HTTPException(status_code=400,
                                    detail=f"Карты с номером {loyality_transaction_data.loyality_card_number} не существует")

            card_balance = card.balance
            card_dict = {
                "balance": card_balance,
                # "cashbox": user.cashbox_id
            }

            if loyality_transaction_data.type == "accrual":
                card_dict["balance"] = round(
                    float(card_dict["balance"]) + float(loyality_transaction_data.amount), 2
                )
            elif loyality_transaction_data.type == "withdraw":
                card_dict["balance"] = round(
                    float(card_dict["balance"]) - float(loyality_transaction_data.amount), 2
                )

            if card.status_card:
                q = loyality_cards.update().where(loyality_cards.c.id == card.id).values(card_dict)
                await database.execute(q)
            else:
                raise HTTPException(status_code=403, detail="Данная карта заблокирована!")

            inserted_ids = set()

            if not loyality_transactions_values.get("dated"):
                loyality_transactions_values['dated'] = datetime.now().replace(hour=0, minute=0, second=0,
                                                                               microsecond=0)
            else:
                loyality_transactions_values["dated"] = datetime.fromtimestamp(loyality_transactions_values["dated"])

            if loyality_transactions_values.get("preamount") and loyality_transactions_values.get("percentamount"):
                loyality_transactions_values['amount'] = float(loyality_transaction_data.preamount) * float(
                    loyality_transaction_data.percentamount)

                loyality_transactions_values['amount'] = round(loyality_transactions_values['amount'], 2)

                del loyality_transactions_values['preamount']
                del loyality_transactions_values['percentamount']
            else:
                del loyality_transactions_values['preamount']
                del loyality_transactions_values['percentamount']

            loyality_transactions_values["created_by_id"] = user.id
            loyality_transactions_values['cashbox'] = user.cashbox_id
            loyality_transactions_values["loyality_card_id"] = card.id
            loyality_transactions_values['loyality_card_number'] = clear_phone_number(
                phone_number=loyality_transactions_values['loyality_card_number']
            )
            loyality_transactions_values['card_balance'] = card_balance
            # loyality_transactions_values["dead_at"] = datetime.fromtimestamp(loyality_transactions_values["dead_at"])

            query = loyality_transactions.insert().values(loyality_transactions_values)
            loyality_transaction_id = await database.execute(query)
            inserted_ids.add(loyality_transaction_id)

            query = (
                loyality_transactions.select().where(
                    loyality_transactions.c.created_by_id == user.id,
                    loyality_transactions.c.id.in_(inserted_ids)
                )
            )

            loyality_transactions_db = await database.fetch_all(query)
            loyality_transactions_db = [*map(datetime_to_timestamp, loyality_transactions_db)]

            await manager.send_message(
                token,
                {
                    "action": "create",
                    "target": "loyality_transactions",
                    "result": loyality_transactions_db[0],
                },
            )

            await asyncio.gather(raschet_bonuses(user.cashbox_id))

            # return loyality_transactions_db[0]
            return {**loyality_transactions_db[0], **{"data": {"status": "success"}}}


@router.patch("/loyality_transactions/{idx}/", response_model=schemas.LoyalityTransaction)
async def edit_loyality_transaction(
        token: str,
        idx: int,
        loyality_transaction: schemas.LoyalityTransactionEdit,
):
    """Редактирование транзакций"""
    user = await get_user_by_token(token)
    loyality_transaction_db = await get_entity_by_id_cashbox(loyality_transactions, idx, user.cashbox_id)
    loyality_transaction_values = loyality_transaction.dict(exclude_unset=True)

    if loyality_transaction_values:

        if loyality_transaction_values.get('dated'):
            loyality_transaction_values['dated'] = datetime.fromtimestamp(loyality_transaction_values["dated"])

        if loyality_transaction_values.get('loyality_card_id'):
            if loyality_transaction_values['loyality_card_id'] != loyality_transaction_db.loyality_card_id:
                new_card = await database.fetch_one(loyality_cards.select().where(
                    loyality_cards.c.id == loyality_transaction_values['loyality_card_id'],
                    loyality_cards.c.cashbox_id == user.cashbox_id))
                loyality_transaction_values['loyality_card_number'] = new_card.card_number

        query = (
            loyality_transactions.update()
            .where(loyality_transactions.c.id == idx, loyality_transactions.c.cashbox == user.cashbox_id)
            .values(loyality_transaction_values)
        )
        await database.execute(query)
        loyality_transaction_db = await get_entity_by_id_cashbox(loyality_transactions, idx, user.cashbox_id)

    loyality_transaction_db = datetime_to_timestamp(loyality_transaction_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "loyality_transactions", "result": loyality_transaction_db},
    )

    await asyncio.gather(raschet_bonuses(user.cashbox_id))

    return {**loyality_transaction_db, **{"data": {"status": "success"}}}


@router.delete("/loyality_transactions/{idx}/", response_model=schemas.LoyalityTransaction)
async def delete_loyality_transaction(token: str, idx: int):
    """Удаление транзакций"""
    user = await get_user_by_token(token)

    await get_entity_by_id_cashbox(loyality_transactions, idx, user.cashbox_id)

    query = (
        loyality_transactions.update()
        .where(loyality_transactions.c.id == idx, loyality_transactions.c.cashbox == user.cashbox_id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = loyality_transactions.select().where(
        loyality_transactions.c.id == idx, loyality_transactions.c.cashbox == user.cashbox_id
    )
    loyality_transaction_db = await database.fetch_one(query)
    loyality_transaction_db = datetime_to_timestamp(loyality_transaction_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "loyality_transactions",
            "result": loyality_transaction_db,
        },
    )

    await asyncio.gather(raschet_bonuses(user.cashbox_id))

    return {**loyality_transaction_db, **{"data": {"status": "success"}}}

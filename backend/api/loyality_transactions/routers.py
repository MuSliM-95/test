from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.sql.functions import coalesce

from database.db import database, loyality_transactions, loyality_cards
import api.loyality_transactions.schemas as schemas
from typing import Optional, Dict, Any
from sqlalchemy import desc, func, select, case, and_

from functions.helpers import datetime_to_timestamp, get_filters_transactions, \
    get_entity_by_id_cashbox, clear_phone_number, get_entity_by_id_and_created_by

from ws_manager import manager
from functions.helpers import get_user_by_token

from datetime import datetime
import asyncio

router = APIRouter(tags=["loyality_transactions"])


async def raschet_bonuses(card_id: int) -> None:
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
            loyality_transactions.c.loyality_card_id == card_id,
            loyality_transactions.c.status.is_(True),
            loyality_transactions.c.is_deleted.is_(False)
        )
    )
    edit_dict: Dict[str, Any] = dict(await database.fetch_one(q))

    await database.execute(loyality_cards.update().where(loyality_cards.c.id == card_id).values(edit_dict))


@router.get("/loyality_transactions/{idx}/", response_model=schemas.LoyalityTransaction)
async def get_loyality_transaction_by_id(token: str, idx: int):
    """Получение транзакции по ID"""
    user = await get_user_by_token(token)
    loyality_transactions_db = await get_entity_by_id_and_created_by(loyality_transactions, idx, user.id)
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

    card_idx = (
        select(loyality_transactions.c.loyality_card_id)
        .where(loyality_transactions.c.id == idx)
        .limit(1)
    )
    card_idx = await database.fetch_val(card_idx, column=0)

    await asyncio.gather(asyncio.create_task(raschet_bonuses(card_idx)))

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

    loyality_card_id = loyality_transaction_db.loyality_card_id

    loyality_transaction_db = datetime_to_timestamp(loyality_transaction_db)



    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "loyality_transactions",
            "result": loyality_transaction_db,
        },
    )

    await asyncio.gather(asyncio.create_task(raschet_bonuses(loyality_card_id)))

    return {**loyality_transaction_db, **{"data": {"status": "success"}}}

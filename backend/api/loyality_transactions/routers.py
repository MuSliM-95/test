from fastapi import APIRouter, Depends, HTTPException
from database.db import database, loyality_transactions, loyality_cards
import api.loyality_transactions.schemas as schemas
from typing import Optional
from sqlalchemy import desc

from functions.helpers import datetime_to_timestamp, get_entity_by_id, get_filters_transactions, get_entity_by_id_and_created_by

from ws_manager import manager
from functions.helpers import get_user_by_token

from datetime import datetime
import asyncio

router = APIRouter(tags=["loyality_transactions"])


async def raschet_bonuses(user):
    q = loyality_cards.select().where(loyality_cards.c.cashbox_id == user.cashbox_id, loyality_cards.c.status_card == True, loyality_cards.c.is_deleted == False)
    all_cards = await database.fetch_all(q)

    for card in all_cards:

        balance = 0
        income = 0
        outcome = 0

        q = loyality_transactions.select().where(loyality_transactions.c.loyality_card_id == card.id, loyality_transactions.c.status == True, loyality_transactions.c.is_deleted == False)
        all_transactions_for_card = await database.fetch_all(q)

        for transaction in all_transactions_for_card:
            if transaction.type == "accrual":
                income += transaction.amount
                balance += transaction.amount
            else:
                outcome += transaction.amount
                balance -= transaction.amount

        if balance < 0:
            balance = 0

        edit_dict = {
            "balance": balance,
            "income": income,
            "outcome": outcome
        }

        await database.execute(loyality_cards.update().where(loyality_cards.c.id == card.id).values(edit_dict))

@router.get("/loyality_transactions/{idx}", response_model=schemas.LoyalityTransaction)
async def get_loyality_transaction_by_id(token: str, idx: int):
    """Получение транзакции по ID"""
    user = await get_user_by_token(token)
    loyality_transactions_db = await get_entity_by_id(loyality_transactions, idx, user.id)
    loyality_transactions_db = datetime_to_timestamp(loyality_transactions_db)

    return loyality_transactions_db


@router.get("/loyality_transactions/", response_model=schemas.CountRes)
async def get_transactions(token: str, limit: int = 100, offset: int = 0, filters_q: schemas.LoyalityTranstactionFilters = Depends()):
    """Получение списка транзакций"""
    user = await get_user_by_token(token)

    filters = get_filters_transactions(loyality_transactions, filters_q)

    if filters:
        query = (
            loyality_transactions.select()
            .where(
                loyality_transactions.c.created_by_id == user.id,
                loyality_transactions.c.is_deleted.is_not(True)
                
            )
            .order_by(desc(loyality_transactions.c.id))
            .filter(*filters)
            .limit(limit)
            .offset(offset)
        )
    else:
        query = (
            loyality_transactions.select()
            .where(
                loyality_transactions.c.created_by_id == user.id,
                loyality_transactions.c.is_deleted.is_not(True)
                
            )
            .order_by(desc(loyality_transactions.c.id))
            .limit(limit)
            .offset(offset)
        )


    loyality_transactions_db = await database.fetch_all(query)
    loyality_transactions_db = [*map(datetime_to_timestamp, loyality_transactions_db)]

    c = f"SELECT count(*) FROM loyality_transactions WHERE loyality_transactions.created_by_id = {user.id} AND loyality_transactions.is_deleted = false"
    count = await database.fetch_one(c)

    return {"result": loyality_transactions_db, "count": dict(count)['count']} # LOCAL

    # return loyality_transactions_db


@router.post("/loyality_transactions/", response_model=Optional[schemas.LoyalityTransaction])
async def create_loyality_transaction(token: str, loyality_transaction_data: schemas.LoyalityTransactionCreate):
    """Создание транзакций"""
    user = await get_user_by_token(token)

    if user:
        if user.status:
            if loyality_transaction_data.loyality_card_number:
                    
                    q = loyality_cards.select().where(loyality_cards.c.card_number == loyality_transaction_data.loyality_card_number, loyality_cards.c.cashbox_id == user.cashbox_id, loyality_cards.c.is_deleted == False)
                    card = await database.fetch_one(q)

                    if not card:
                        raise HTTPException(403, "Данная карта не найдена")

                    card_dict = {
                        "balance": card.balance,
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


    loyality_transactions_values = loyality_transaction_data.dict()

    if not loyality_transactions_values.get("dated"):
        loyality_transactions_values['dated'] = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        loyality_transactions_values["dated"] = datetime.fromtimestamp(loyality_transactions_values["dated"])

    if loyality_transactions_values.get("preamount") and loyality_transactions_values.get("percentamount"):
        loyality_transactions_values['amount'] = float(loyality_transaction_data.preamount) * float(loyality_transaction_data.percentamount)

        loyality_transactions_values['amount'] = round(loyality_transactions_values['amount'], 2)

        del loyality_transactions_values['preamount']
        del loyality_transactions_values['percentamount']
    else:
        del loyality_transactions_values['preamount']
        del loyality_transactions_values['percentamount']

    loyality_transactions_values["created_by_id"] = user.id
    loyality_transactions_values["loyality_card_id"] = card.id
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

    asyncio.create_task(raschet_bonuses(user))

    # return loyality_transactions_db[0]
    return {**loyality_transactions_db[0],  **{ "data": { "status": "success" } }}


@router.patch("/loyality_transactions/{idx}", response_model=schemas.LoyalityTransaction)
async def edit_loyality_transaction(
    token: str,
    idx: int,
    loyality_transaction: schemas.LoyalityTransactionEdit,
):
    """Редактирование транзакций"""
    user = await get_user_by_token(token)
    loyality_transaction_db = await get_entity_by_id_and_created_by(loyality_transactions, idx, user.id)
    loyality_transaction_values = loyality_transaction.dict(exclude_unset=True)

    if loyality_transaction_values:

        if loyality_transaction_values.get('dated'):
            loyality_transaction_values['dated'] = datetime.fromtimestamp(loyality_transaction_values["dated"])
        
        if loyality_transaction_values.get('loyality_card_id'):
            if loyality_transaction_values['loyality_card_id'] != loyality_transaction_db.loyality_card_id:
                new_card = await database.fetch_one(loyality_cards.select().where(loyality_cards.c.id == loyality_transaction_values['loyality_card_id'], loyality_cards.c.cashbox_id == user.cashbox_id ))
                loyality_transaction_values['loyality_card_number'] = new_card.card_number

        query = (
            loyality_transactions.update()
            .where(loyality_transactions.c.id == idx, loyality_transactions.c.created_by_id == user.id)
            .values(loyality_transaction_values)
        )
        await database.execute(query)
        loyality_transaction_db = await get_entity_by_id_and_created_by(loyality_transactions, idx, user.id)

    loyality_transaction_db = datetime_to_timestamp(loyality_transaction_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "loyality_transactions", "result": loyality_transaction_db},
    )

    asyncio.create_task(raschet_bonuses(user))

    return {**loyality_transaction_db,  **{ "data": { "status": "success" } }}


@router.delete("/loyality_transactions/{idx}", response_model=schemas.LoyalityTransaction)
async def delete_loyality_transaction(token: str, idx: int):
    """Удаление транзакций"""
    user = await get_user_by_token(token)

    await get_entity_by_id_and_created_by(loyality_transactions, idx, user.id)

    query = (
        loyality_transactions.update()
        .where(loyality_transactions.c.id == idx, loyality_transactions.c.created_by_id == user.id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = loyality_transactions.select().where(
        loyality_transactions.c.id == idx, loyality_transactions.c.created_by_id == user.id
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

    asyncio.create_task(raschet_bonuses(user))

    return {**loyality_transaction_db,  **{ "data": { "status": "success" } }}
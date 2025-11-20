from datetime import datetime, timedelta

from sqlalchemy import select, func

import texts
from bot import bot
from const import PAID, SUCCESS, BLOCKED, PaymentType
from database.db import accounts_balances, database, transactions, tariffs, users_cboxes_relation, users


async def make_transaction(balance: accounts_balances, price: int, users_quantity: int) -> None:
    now = datetime.utcnow()
    new_balance = balance.balance - price
    query = (
        accounts_balances.update()
        .where(accounts_balances.c.id == balance.id)
        .values(
            {
                "balance": new_balance,
                "tariff_type": PAID,
                "updated_at": int(now.timestamp()),
            }
        )
    )
    await database.execute(query)
    query = transactions.insert(
        {
            "cashbox": balance.cashbox,
            "tariff": balance.tariff,
            "users": users_quantity,
            "amount": price,
            "status": SUCCESS,
            "type": PaymentType.outgoing,
            "is_manual_deposit": False,
            "created_at": int(datetime.utcnow().timestamp()),
            "updated_at": int(datetime.utcnow().timestamp()),
        }
    )
    result = await database.execute(query)
    query = (
        accounts_balances.update()
        .where(accounts_balances.c.id == balance.id)
        .values({"last_transaction": result})
    )
    await database.execute(query)
    
    from database.db import cboxes
    old_cbox_query = cboxes.select().where(cboxes.c.id == balance.cashbox)
    old_cbox = await database.fetch_one(old_cbox_query)
    old_cbox_balance = old_cbox.balance if old_cbox else 0.0
    
    await database.execute(
        cboxes.update()
        .where(cboxes.c.id == balance.cashbox)
        .values({
            "balance": new_balance,
            "updated_at": int(now.timestamp())
        })
    )


async def make_deposit(
    cashbox_id: int,
    amount: float,
    tariff_id: int,
    users_quantity: int,
    is_manual: bool = False,
    status: str = SUCCESS,
) -> int:
    now = datetime.utcnow()
    
    balance_query = accounts_balances.select().where(
        accounts_balances.c.cashbox == cashbox_id
    )
    balance = await database.fetch_one(balance_query)
    
    if not balance:
        balance_query = accounts_balances.insert().values(
            cashbox=cashbox_id,
            tariff=tariff_id,
            balance=0,
            tariff_type=PAID,
            created_at=int(now.timestamp()),
            updated_at=int(now.timestamp()),
        )
        balance_id = await database.execute(balance_query)
        balance = await database.fetch_one(
            accounts_balances.select().where(accounts_balances.c.id == balance_id)
        )
    
    old_balance_value = balance.balance
    new_balance = old_balance_value + amount
    update_query = (
        accounts_balances.update()
        .where(accounts_balances.c.id == balance.id)
        .values(
            {
                "balance": new_balance,
                "tariff_type": PAID,
                "updated_at": int(now.timestamp()),
            }
        )
    )
    await database.execute(update_query)
    
    transaction_query = transactions.insert().values(
        {
            "cashbox": cashbox_id,
            "tariff": tariff_id,
            "users": users_quantity,
            "amount": amount,
            "status": status,
            "type": PaymentType.incoming,
            "is_manual_deposit": is_manual,
            "created_at": int(now.timestamp()),
            "updated_at": int(now.timestamp()),
        }
    )
    transaction_id = await database.execute(transaction_query)
    
    update_query = (
        accounts_balances.update()
        .where(accounts_balances.c.id == balance.id)
        .values({"last_transaction": transaction_id})
    )
    await database.execute(update_query)
    
    from database.db import cboxes
    old_cbox_query = cboxes.select().where(cboxes.c.id == cashbox_id)
    old_cbox = await database.fetch_one(old_cbox_query)
    old_cbox_balance = old_cbox.balance if old_cbox else 0.0
    
    await database.execute(
        cboxes.update()
        .where(cboxes.c.id == cashbox_id)
        .values({
            "balance": new_balance,
            "updated_at": int(now.timestamp())
        })
    )
    
    return transaction_id


async def recalculate_account_balance(cashbox_id: int) -> dict:
    incoming_query = select(func.sum(transactions.c.amount)).where(
        transactions.c.cashbox == cashbox_id,
        transactions.c.type == PaymentType.incoming,
        transactions.c.status == SUCCESS
    )
    total_incoming = await database.execute(incoming_query)
    total_incoming = float(total_incoming or 0)
    
    outgoing_query = select(func.sum(transactions.c.amount)).where(
        transactions.c.cashbox == cashbox_id,
        transactions.c.type == PaymentType.outgoing,
        transactions.c.status == SUCCESS
    )
    total_outgoing = await database.execute(outgoing_query)
    total_outgoing = float(total_outgoing or 0)
    
    calculated_balance = round(total_incoming - total_outgoing, 2)
    
    balance_query = accounts_balances.select().where(
        accounts_balances.c.cashbox == cashbox_id
    )
    current_balance = await database.fetch_one(balance_query)
    current_balance_value = current_balance.balance if current_balance else 0.0
    
    all_transactions_query = transactions.select().where(
        transactions.c.cashbox == cashbox_id,
        transactions.c.status == SUCCESS
    ).order_by(transactions.c.created_at.desc())
    all_transactions = await database.fetch_all(all_transactions_query)
    
    all_transactions_all_statuses_query = transactions.select().where(
        transactions.c.cashbox == cashbox_id
    ).order_by(transactions.c.created_at.desc())
    all_transactions_all_statuses = await database.fetch_all(all_transactions_all_statuses_query)
    
    result = {
        "cashbox_id": cashbox_id,
        "current_balance_in_db": current_balance_value,
        "calculated_balance": calculated_balance,
        "difference": round(current_balance_value - calculated_balance, 2),
        "total_incoming": total_incoming,
        "total_outgoing": total_outgoing,
        "transactions_count": len(all_transactions),
        "transactions": [
            {
                "id": t.id,
                "type": t.type,
                "amount": t.amount,
                "status": t.status,
                "created_at": t.created_at,
                "external_id": t.external_id
            }
            for t in all_transactions
        ]
    }
    
    return result


async def fix_account_balance(cashbox_id: int, auto_fix: bool = False) -> dict:
    recalculation = await recalculate_account_balance(cashbox_id)
    
    difference = abs(recalculation['difference'])
    calculated_balance = recalculation['calculated_balance']
    
    if difference > 0.01 and auto_fix:
        balance_query = accounts_balances.select().where(
            accounts_balances.c.cashbox == cashbox_id
        )
        current_balance = await database.fetch_one(balance_query)
        
        if not current_balance:
            first_transaction = await database.fetch_one(
                transactions.select().where(
                    transactions.c.cashbox == cashbox_id
                ).order_by(transactions.c.created_at.asc()).limit(1)
            )
            tariff_id = first_transaction.tariff if first_transaction else 1
            
            balance_query = accounts_balances.insert().values(
                cashbox=cashbox_id,
                tariff=tariff_id,
                balance=calculated_balance,
                tariff_type=PAID,
                created_at=int(datetime.utcnow().timestamp()),
                updated_at=int(datetime.utcnow().timestamp()),
            )
            await database.execute(balance_query)
        else:
            old_balance = current_balance.balance
            update_query = (
                accounts_balances.update()
                .where(accounts_balances.c.id == current_balance.id)
                .values({
                    "balance": calculated_balance,
                    "updated_at": int(datetime.utcnow().timestamp())
                })
            )
            await database.execute(update_query)
        
        from database.db import cboxes
        old_cbox_query = cboxes.select().where(cboxes.c.id == cashbox_id)
        old_cbox = await database.fetch_one(old_cbox_query)
        old_cbox_balance = old_cbox.balance if old_cbox else 0.0
        
        update_cbox_query = (
            cboxes.update()
            .where(cboxes.c.id == cashbox_id)
            .values({
                "balance": calculated_balance,
                "updated_at": int(datetime.utcnow().timestamp())
            })
        )
        await database.execute(update_cbox_query)
        
        recalculation['fixed'] = True
        recalculation['old_balance'] = recalculation['current_balance_in_db']
        recalculation['new_balance'] = calculated_balance
    else:
        recalculation['fixed'] = False
        
        balance_query = accounts_balances.select().where(
            accounts_balances.c.cashbox == cashbox_id
        )
        current_balance = await database.fetch_one(balance_query)
        if current_balance:
            from database.db import cboxes
            old_cbox_query = cboxes.select().where(cboxes.c.id == cashbox_id)
            old_cbox = await database.fetch_one(old_cbox_query)
            old_cbox_balance = old_cbox.balance if old_cbox else 0.0
            
            if abs(old_cbox_balance - current_balance.balance) > 0.01:
                update_cbox_query = (
                    cboxes.update()
                    .where(cboxes.c.id == cashbox_id)
                    .values({
                        "balance": current_balance.balance,
                        "updated_at": int(datetime.utcnow().timestamp())
                    })
                )
                await database.execute(update_cbox_query)
    
    return recalculation


async def make_account(balance: accounts_balances) -> None:
    """Checks if paid period finished"""
    now = datetime.utcnow()
    balance_tariff = await database.fetch_one(
        tariffs.select().where(tariffs.c.id == balance.tariff)
    )

    # checking if paid period finished or not
    if balance.last_transaction:
        transaction = await database.fetch_one(
            transactions.select().where(transactions.c.id == balance.last_transaction)
        )
        if datetime.fromtimestamp(transaction.updated_at) + timedelta(days=balance_tariff.frequency) > now:
            return

    count_query = (
        select(func.count(users_cboxes_relation.c.id))
        .where(users_cboxes_relation.c.cashbox_id == balance.cashbox)
    )
    users_quantity = await database.execute(count_query)
    price = balance_tariff.price / balance_tariff.frequency
    if balance_tariff.per_user:
        price *= users_quantity

    # making new transaction
    if price and balance.balance >= price:
        await make_transaction(balance, price, users_quantity)
        return

    # if user has insufficient funds blocking account and sending a message
    query = (
        accounts_balances.update()
        .where(accounts_balances.c.id == balance.id)
        .values({"tariff_type": BLOCKED, "updated_at": int(now.timestamp())})
    )
    await database.execute(query)

    get_users_id = select(users_cboxes_relation.c.user).where(
        users_cboxes_relation.c.cashbox_id == balance.cashbox
    )
    query = users.select().where(users.c.id == get_users_id.scalar_subquery())
    user = await database.fetch_one(query)
    chat_id = user.chat_id
    await bot.send_message(
        chat_id,
        texts.balance_blocked.format(
            tariff=balance_tariff.name,
            users=users_quantity,
            per_user=balance_tariff.price if balance_tariff.per_user else 0,
            total=price,
            link=texts.url_link_pay.format(user_id=user.owner_id, cashbox_id=balance.cashbox),
        ),
    )

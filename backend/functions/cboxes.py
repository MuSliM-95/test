from datetime import datetime
from sqlalchemy import func, select

from functions.helpers import gen_token
from database.db import database, cboxes, pboxes, users, users_cboxes_relation, price_types, cashbox_settings, accounts_balances, tariffs
from const import DEMO

import websockets
import json


async def create_cbox(user):
    created = int(datetime.utcnow().timestamp())
    updated = int(datetime.utcnow().timestamp())

    created_date = datetime.utcnow().date()
    created_date_ts = int(datetime.timestamp(datetime.combine(created_date, datetime.min.time())))

    invite_token = gen_token()
    cashbox_query = cboxes.insert().values(
        balance=0,
        admin=user.id,
        invite_token=invite_token,
        created_at=created,
        updated_at=updated
    )

    cashbox_id = await database.execute(cashbox_query)

    settings_query = cashbox_settings.insert().values(
        cashbox_id=cashbox_id,
        require_photo_for_writeoff=False,
        created_at=datetime.now(),
        updated_at=datetime.now(),
        is_deleted=False
    )
    await database.execute(settings_query)

    cashbox_query = cboxes.select().where(cboxes.c.id == cashbox_id)
    cashbox = await database.fetch_one(cashbox_query)

    paybox = pboxes.insert().values(
        start_balance=0,
        balance=0,
        name="default",
        balance_date=created_date_ts,
        update_start_balance=created_date_ts,
        update_start_balance_date=created_date_ts,
        cashbox=cashbox.id,
        created_at=created,
        updated_at=created
    )

    cbox_update_name = cboxes.update().where(cboxes.c.id == cashbox.id).values(
        name=f"{user.first_name}_{cashbox.id}")
    await database.execute(cbox_update_name)
    await database.execute(paybox)

    rel_token = gen_token()

    relship = users_cboxes_relation.insert().values(
        user=user.id,
        cashbox_id=cashbox.id,
        token=rel_token,
        status=True,
        is_owner=True,
        created_at=created,
        updated_at=updated
    )

    rl_id = await database.execute(relship)
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.id == rl_id)
    rl = await database.fetch_one(query)

    query = price_types.insert().values(
        name="chatting",
        owner=rl.id,
        cashbox=cashbox.id,
        is_system=True
    )
    await database.execute(query)

    try:
        tariff_query = (
            tariffs.select()
            .where(tariffs.c.actual == True)
            .order_by(tariffs.c.price.asc())
        )
        tariff = await database.fetch_one(tariff_query)
        
        if tariff:
            balance_query = accounts_balances.insert().values(
                cashbox=cashbox.id,
                balance=0,
                tariff=tariff.id,
                tariff_type=DEMO,
                created_at=created,
                updated_at=created,
            )
            await database.execute(balance_query)
    except Exception as e:
        print(f"Warning: Failed to create balance for cashbox {cashbox.id}: {e}")

    return rl


async def update_cashbox_balance(cashbox_id: int) -> None:
    try:
        sum_query = select(func.sum(pboxes.c.balance)).where(
            pboxes.c.cashbox == cashbox_id
        )
        total_balance = await database.execute(sum_query)
        
        if total_balance is None:
            total_balance = 0.0
        else:
            total_balance = round(float(total_balance), 2)
        
        current_cbox = await database.fetch_one(
            cboxes.select().where(cboxes.c.id == cashbox_id)
        )
        old_balance = current_cbox.balance if current_cbox else 0.0
        
        if old_balance != total_balance:
            update_query = (
                cboxes.update()
                .where(cboxes.c.id == cashbox_id)
                .values({
                    "balance": total_balance,
                    "updated_at": int(datetime.utcnow().timestamp())
                })
            )
            await database.execute(update_query)
    except Exception as e:
        import traceback
        print(f"Error updating cashbox balance for cashbox {cashbox_id}: {e}")
        print(f"Traceback: {traceback.format_exc()}")


async def join_cbox(user, cbox):
    rel_token = gen_token()

    created = int(datetime.utcnow().timestamp())
    updated = int(datetime.utcnow().timestamp())

    relship = users_cboxes_relation.insert().values(
        user=user.id,
        cashbox_id=cbox['id'],
        token=rel_token,
        status=True,
        is_owner=False,
        created_at=created,
        updated_at=updated
    )

    rl_id = await database.execute(relship)

    query = users_cboxes_relation.select().where(users_cboxes_relation.c.id == rl_id)
    rl = await database.fetch_one(query)

    q = users.select().where(users.c.id == user.id)
    user = await database.fetch_one(q)

    user_dict = {"id": user.id, "first_name": user.first_name, "last_name": user.last_name, "username": user.username,
                 "status": user.status, "photo": user.photo, "is_admin": user.is_admin}

    cbox_id = rl.cashbox_id

    query = users_cboxes_relation.select().where(users_cboxes_relation.c.cashbox_id == cbox_id)
    all_rl = await database.fetch_all(query)

    tokens_list = [rel.token for rel in all_rl]

    try:
        async with websockets.connect(f"wss://app.tablecrm.com/ws/{rel_token}") as ws:
            await ws.send(json.dumps({
                "super_secret_token": "143a2854998b0c3ab1e0f38b5a66d12024cd088b9eac8ae39df6161313d254fd",
                "tokens_list": tokens_list,
                "user": {"action": "users_create", "result": user_dict}
            }))

            await ws.close()
    except Exception as error:
        print(f"Exception when send websocket add new user: {error}")

    return rl

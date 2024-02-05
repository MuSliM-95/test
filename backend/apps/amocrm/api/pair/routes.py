from fastapi import APIRouter, HTTPException

from apps.amocrm.function import refresh_token
from ws_manager import manager

from datetime import datetime
from jobs import scheduler
from functions.helpers import gen_token

from database.db import database, amo_install, amo_install_table_cashboxes, users_cboxes_relation

router = APIRouter(tags=["amocrm"])


@router.get("/integration_pair/")
async def sc_l(token: str, amo_token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query=query)
    if user:

        query = amo_install.select().where(amo_install.c.pair_token == amo_token)
        a_t = await database.fetch_one(query=query)

        if a_t:
            query = amo_install_table_cashboxes.select().where(
                amo_install_table_cashboxes.c.amo_integration_id == a_t["id"])
            amo_pair = await database.fetch_one(query=query)

            time = int(datetime.utcnow().timestamp())
            integration_data = {}

            if not amo_pair:
                integration_data["cashbox_id"] = user["cashbox_id"]
                integration_data["amo_integration_id"] = a_t["id"]
                integration_data["last_token"] = amo_token
                integration_data["status"] = a_t["active"]
                integration_data["created_at"] = time
                integration_data["updated_at"] = time

                query = amo_install_table_cashboxes.insert().values(integration_data)
                await database.execute(query)

            else:
                integration_data["last_token"] = amo_token
                integration_data["updated_at"] = time

                query = amo_install_table_cashboxes.update().where(
                    amo_install_table_cashboxes.c.amo_integration_id == a_t["id"]).values(integration_data)
                await database.execute(query)

            await manager.send_message(user.token,
                                       {"action": "paired", "target": "integrations", "integration_status": True})

            return {"status": "success"}

        else:
            raise HTTPException(
                status_code=403, detail="Вы ввели некорректный токен амо!"
            )

    else:
        raise HTTPException(
            status_code=403, detail="Вы ввели некорректный токен!"
        )


@router.get("/get_my_token/")
async def sc_l(referer: str):
    query = amo_install.select().where(amo_install.c.referrer == referer)
    user = await database.fetch_one(query)
    if user:
        return {"token": user["pair_token"]}
    else:
        return {"status": "incorrect token!"}


@router.get("/refresh_my_token/")
async def sc_l(referer: str):
    query = amo_install.select().where(amo_install.c.referrer == referer)
    user = await database.fetch_one(query)
    if user:

        new_token = gen_token()
        new_pair_token = {"pair_token": new_token}

        query = amo_install.update().where(amo_install.c.referrer == referer).values(new_pair_token)
        await database.execute(query)

        query = amo_install.select().where(amo_install.c.referrer == referer)
        install_id = await database.fetch_one(query)

        query = amo_install_table_cashboxes.select().where(
            amo_install_table_cashboxes.c.amo_integration_id == install_id.id)
        pair = await database.fetch_one(query)

        query = users_cboxes_relation.select().where(users_cboxes_relation.c.cashbox_id == pair['cashbox_id'])
        cashbox = await database.fetch_one(query)

        await manager.send_message(cashbox.token, {"action": "paired", "target": "integrations",
                                                   "integration_status": "need_to_refresh"})

        return {"token": new_token}
    else:
        return {"status": "incorrect token!"}


@router.get("/check_pair/")
async def sc_l(token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query=query)
    if user:

        query = amo_install_table_cashboxes.select().where(
            amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"])
        pair = await database.fetch_one(query=query)

        if pair:
            query = amo_install.select().where(amo_install.c.id == pair["amo_integration_id"])
            amo_int = await database.fetch_one(query)
            if pair["last_token"] != amo_int["pair_token"]:
                return {"result": "paired", "integration_status": "need_to_refresh"}
            else:
                return {"result": "paired", "integration_status": pair['status']}
        else:
            return {"result": "not paired"}
    else:
        return {"status": "incorrect token!"}


@router.get("/integration_unpair/")
async def sc_l(token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        query = amo_install_table_cashboxes.select().where(
            amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"])
        pair = await database.fetch_one(query)

        query = amo_install.select().where(amo_install.c.id == pair["amo_integration_id"])
        a_t = await database.fetch_one(query)

        pair_dict = dict(pair)
        pair_dict["status"] = False
        pair_dict["updated_at"] = int(datetime.utcnow().timestamp())

        query = amo_install_table_cashboxes.update().where(
            amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"]).values(pair_dict)
        await database.fetch_one(query)

        db_dict = dict(a_t)
        db_dict["active"] = False
        db_dict["updated_at"] = int(datetime.utcnow().timestamp())

        query = amo_install.update().where(amo_install.c.id == pair["amo_integration_id"]).values(db_dict)
        await database.execute(query)

        if scheduler.get_job(db_dict["referrer"]):
            scheduler.remove_job(db_dict["referrer"])

        await manager.send_message(user.token,
                                   {"action": "paired", "target": "integrations", "integration_status": False})

    else:
        return {"status": "incorrect token!"}


@router.get("/integration_on/")
async def sc_l(token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query=query)
    if user:
        query = amo_install_table_cashboxes.select().where(
            amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"])
        pair = await database.fetch_one(query=query)

        pair_dict = dict(pair)
        pair_dict["status"] = True
        pair_dict["updated_at"] = int(datetime.utcnow().timestamp())

        query = amo_install_table_cashboxes.update().where(
            amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"]).values(pair_dict)
        await database.fetch_one(query=query)

        query = amo_install.select().where(amo_install.c.id == pair["amo_integration_id"])
        a_t = await database.fetch_one(query=query)

        db_dict = dict(a_t)
        db_dict["active"] = True

        query = amo_install.update().where(amo_install.c.id == pair["amo_integration_id"]).values(db_dict)
        await database.execute(query)

        if not scheduler.get_job(db_dict['referrer']):
            scheduler.add_job(refresh_token, trigger="interval", seconds=db_dict['expires_in'], id=db_dict['referrer'],
                              args=[db_dict['referrer']])

        await manager.send_message(user.token, {"action": "paired", "target": "integrations",
                                                "integration_status": pair_dict["status"]})
    else:
        return {"status": "incorrect token!"}


@router.post("/test/")
async def test_endpoint():
    pass

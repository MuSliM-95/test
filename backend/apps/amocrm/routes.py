from fastapi import APIRouter, HTTPException

from ws_manager import manager

from datetime import datetime
import aiohttp
from jobs import scheduler
from functions.helpers import gen_token

from database.db import database, users, amo_install, amo_install_table_cashboxes, users_cboxes_relation, cboxes, events

router = APIRouter(tags=["amocrm"])


async def refresh_token(referer):
    query = amo_install.select().where(amo_install.c.referrer == referer)
    amo_db_referer = await database.fetch_one(query=query)

    if amo_db_referer:
        if amo_db_referer["active"]:
            amo_post_json = {
                "client_id": amo_db_referer['client_id'],
                "client_secret": amo_db_referer['client_id'],
                "grant_type": "refresh_token",
                "refresh_token": amo_db_referer['refresh_token'],
                "redirect_uri": "https://app.tablecrm.com/api/v1/amo_connect"
            }

            q = amo_install_table_cashboxes.select().where(amo_install_table_cashboxes.c.amo_integration_id == amo_db_referer.id)
            amo_tablecrm_rel = await database.fetch_one(query)

            q = cboxes.select().where(cboxes.c.id == amo_tablecrm_rel.cashbox_id)
            cashbox = await database.fetch_one(query)

            async with aiohttp.ClientSession() as session1:
                async with session1.post(f'https://{referer}/oauth2/access_token', json=amo_post_json) as resp:
                    amo_resp_json = await resp.json()
                    # event_body = {
                    #     "type": "amoevent",
                    #     "name": "Обновление Access токена",
                    #     "url": resp.url,
                    #     "payload": amo_post_json,
                    #     "cashbox_id": cashbox.id,
                    #     "user_id": cashbox.admin,
                    #     "token": None,
                    #     "ip": "https://app.tablecrm.com/",
                    #     "promoimage": None,
                    #     "promodata": None,
                    #     "method": "POST",
                    # }
                    # await database.execute(
                    #     events.insert().values(event_body)
                    # )
                await session1.close()
            amo_db_referer_dict = dict(amo_db_referer)
            amo_db_referer_dict["access_token"] = amo_resp_json['access_token']
            amo_db_referer_dict["updated_at"] = int(datetime.utcnow().timestamp())

            query = amo_install.update().where(amo_install.c.referrer == referer).values(amo_db_referer_dict)
            await database.execute(query)

            return amo_resp_json['access_token']


@router.get("/amo_connect")
async def sc_l(code: str, referer: str, platform: int, client_id: str, from_widget: str):
    user = True
    if user:
        
        query = amo_install.select().where(amo_install.c.referrer == referer)
        install = await database.fetch_one(query)

        client_secret = "Xm3bf8KSyMUD0flV6dZkTp8Dx1aT21TtzhAkM9EH8ljglw4DTcfJAN2RdiJ6Jpw6"

        amo_post_json = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": "https://app.tablecrm.com/api/v1/amo_connect"
        }

        if install:
            if not install["active"]:
                amo_db_data = dict(install)
                async with aiohttp.ClientSession() as session1:
                    async with session1.post(f'https://{referer}/oauth2/access_token', json=amo_post_json) as resp:
                        amo_resp_json1 = await resp.json()
                    await session1.close()

                timestamp = int(datetime.utcnow().timestamp())

                amo_db_data["code"] = code
                amo_db_data["access_token"] = amo_resp_json1["access_token"]
                amo_db_data["refresh_token"] = amo_resp_json1["refresh_token"]
                amo_db_data["active"] = True
                amo_db_data["updated_at"] = timestamp

                query = amo_install.update().where(amo_install.c.referrer == referer).values(amo_db_data)
                await database.execute(query)

                integration_dict = {"status": True, "updated_at": timestamp}

                query = amo_install_table_cashboxes.update().where(amo_install_table_cashboxes.c.amo_integration_id == install["id"]).values(integration_dict)
                await database.execute(query=query)

                query = amo_install_table_cashboxes.select().where(amo_install_table_cashboxes.c.amo_integration_id == install["id"])
                cashbox_id = await database.fetch_one(query=query)

                query = cboxes.select().where(cboxes.c.id == cashbox_id['cashbox_id'])
                cashbox = await database.fetch_one(query=query)

                # await manager.send_message(cashbox["token"], {"result": "paired", "integration_status": True})
                await manager.send_message(cashbox.token, {"action": "paired", "target": "integrations", "integration_status": True})

                if not scheduler.get_job(referer):
                    scheduler.add_job(refresh_token, trigger="interval", seconds=amo_db_data["expires_in"], id=referer, args=[referer])

            return {"status": "amo token already connected!"}
        else:

            async with aiohttp.ClientSession() as session1:
                async with session1.post(f'https://{referer}/oauth2/access_token', json=amo_post_json) as resp:
                    amo_resp_json1 = await resp.json()
                await session1.close()
            amo_token = amo_resp_json1.get("access_token")
            if amo_token:
                headers = {'Authorization': f'Bearer {amo_token}'}
                async with aiohttp.ClientSession(headers=headers) as session2:
                    async with session2.get(f'https://{referer}/api/v4/account') as resp:
                        amo_resp_json2 = await resp.json()
                    await session2.close()

            amo_data = amo_post_json
            time = int(datetime.utcnow().timestamp())
            amo_data.pop("grant_type")
            amo_data.pop("redirect_uri")
            amo_data["referrer"] = referer
            amo_data["amo_account_id"] = int(amo_resp_json2["id"])
            amo_data["active"] = True
            amo_data["platform"] = platform
            amo_data["created_at"] = time
            amo_data["updated_at"] = time
            amo_data["pair_token"] = gen_token()
            amo_data["from_widget"] = int(from_widget)
            amo_data["expires_in"] = int(amo_resp_json1["expires_in"])
            amo_data["refresh_token"] = amo_resp_json1["refresh_token"]
            amo_data["access_token"] = amo_token
            
            query = amo_install.insert().values(amo_data)
            await database.execute(query)

            scheduler.add_job(refresh_token, trigger="interval", seconds=amo_data["expires_in"], id=referer, args=[referer])

            return {"result": "amo token connected succesfully"}
        
    else:
        return {"status": "incorrect token!"}


@router.get("/amo_disconnect")
async def sc_l(account_id: int, client_uuid: str):
    user = True
    if user:
        
        query = amo_install.select().where(amo_install.c.amo_account_id == account_id and amo_install.c.client_id == client_uuid)
        a_t = await database.fetch_one(query)

        if a_t:
            db_dict = dict(a_t)
            db_dict["active"] = False
            db_dict["updated_at"] = int(datetime.utcnow().timestamp())

            query = amo_install.update().where(amo_install.c.amo_account_id == account_id and amo_install.c.client_id == client_uuid).values(db_dict)
            await database.execute(query)

            integration_dict = {"status": False, "updated_at": int(datetime.utcnow().timestamp())}

            query = amo_install_table_cashboxes.update().where(amo_install_table_cashboxes.c.amo_integration_id == a_t["id"]).values(integration_dict)
            await database.execute(query)

            query = amo_install_table_cashboxes.select().where(amo_install_table_cashboxes.c.amo_integration_id == a_t["id"])
            relship = await database.fetch_one(query)

            query = users_cboxes_relation.select().where(users_cboxes_relation.c.cashbox_id == relship["cashbox_id"])
            cashboxes = await database.fetch_all(query)

            for cashbox in cashboxes:
                await manager.send_message(cashbox.token, {"action": "paired", "target": "integrations", "integration_status": False})

            if scheduler.get_job(db_dict["referrer"]):
                scheduler.remove_job(db_dict["referrer"])

            return {"status": "amo token disconnected succesfully!"}
        else:
            return {"result": "amo token does not connected!"}
        
    else:
        return {"status": "incorrect token!"}



@router.get("/integration_pair")
async def sc_l(token: str, amo_token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query=query)
    if user:
        
        query = amo_install.select().where(amo_install.c.pair_token == amo_token)
        a_t = await database.fetch_one(query=query)

        if a_t:
            query = amo_install_table_cashboxes.select().where(amo_install_table_cashboxes.c.amo_integration_id == a_t["id"])
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

                query = amo_install_table_cashboxes.update().where(amo_install_table_cashboxes.c.amo_integration_id == a_t["id"]).values(integration_data)
                await database.execute(query)

            await manager.send_message(user.token, {"action": "paired", "target": "integrations", "integration_status": True})

            return {"status": "success"}

        else:
            raise HTTPException(
                    status_code=403, detail="Вы ввели некорректный токен амо!"
            )
        
    else:
        raise HTTPException(
            status_code=403, detail="Вы ввели некорректный токен!"
        )


@router.get("/get_my_token")
async def sc_l(referer: str):
    query = amo_install.select().where(amo_install.c.referrer == referer)
    user = await database.fetch_one(query)
    if user:
        return {"token": user["pair_token"]}
    else:
        return {"status": "incorrect token!"}


@router.get("/refresh_my_token")
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

        query = amo_install_table_cashboxes.select().where(amo_install_table_cashboxes.c.amo_integration_id == install_id.id)
        pair = await database.fetch_one(query)

        query = users_cboxes_relation.select().where(users_cboxes_relation.c.cashbox_id == pair['cashbox_id'])
        cashbox = await database.fetch_one(query)

        await manager.send_message(cashbox.token, {"action": "paired", "target": "integrations", "integration_status": "need_to_refresh"})

        return {"token": new_token}
    else:
        return {"status": "incorrect token!"}


@router.get("/check_pair")
async def sc_l(token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query=query)
    if user:
        
        query = amo_install_table_cashboxes.select().where(amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"])
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


@router.get("/integration_unpair")
async def sc_l(token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        query = amo_install_table_cashboxes.select().where(amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"])
        pair = await database.fetch_one(query)

        query = amo_install.select().where(amo_install.c.id == pair["amo_integration_id"])
        a_t = await database.fetch_one(query)

        pair_dict = dict(pair)
        pair_dict["status"] = False
        pair_dict["updated_at"] = int(datetime.utcnow().timestamp())

        query = amo_install_table_cashboxes.update().where(amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"]).values(pair_dict)
        await database.fetch_one(query)

        db_dict = dict(a_t)
        db_dict["active"] = False
        db_dict["updated_at"] = int(datetime.utcnow().timestamp())

        query = amo_install.update().where(amo_install.c.id == pair["amo_integration_id"]).values(db_dict)
        await database.execute(query)

        if scheduler.get_job(db_dict["referrer"]):
            scheduler.remove_job(db_dict["referrer"])

        await manager.send_message(user.token, {"action": "paired", "target": "integrations", "integration_status": False})

    else:
        return {"status": "incorrect token!"}


@router.get("/integration_on")
async def sc_l(token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query=query)
    if user:
        query = amo_install_table_cashboxes.select().where(amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"])
        pair = await database.fetch_one(query=query)

        pair_dict = dict(pair)
        pair_dict["status"] = True
        pair_dict["updated_at"] = int(datetime.utcnow().timestamp())

        query = amo_install_table_cashboxes.update().where(amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"]).values(pair_dict)
        await database.fetch_one(query=query)

        query = amo_install.select().where(amo_install.c.id == pair["amo_integration_id"])
        a_t = await database.fetch_one(query=query)

        db_dict = dict(a_t)
        db_dict["active"] = True

        query = amo_install.update().where(amo_install.c.id == pair["amo_integration_id"]).values(db_dict)
        await database.execute(query)

        if not scheduler.get_job(db_dict['referrer']):
            scheduler.add_job(refresh_token, trigger="interval", seconds=db_dict['expires_in'], id=db_dict['referrer'], args=[db_dict['referrer']])

        await manager.send_message(user.token, {"action": "paired", "target": "integrations", "integration_status": pair_dict["status"]})
    else:
        return {"status": "incorrect token!"}
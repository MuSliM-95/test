from datetime import datetime

import aiohttp
from fastapi import HTTPException, APIRouter
from starlette import status

from apps.amocrm.function import refresh_token, add_amo_install, update_amo_install, add_job_compare
from apps.amocrm.tasks.contacts import compare_contacts
from database.db import amo_install, database, settings, amo_install_table_cashboxes, cboxes, users_cboxes_relation
from functions.helpers import gen_token
from jobs import scheduler
from ws_manager import manager

router = APIRouter(tags=["amocrm"])


@router.get("/amo_connect")
async def sc_l(code: str, referer: str, platform: int, client_id: str, from_widget: str):
    user = True
    if user:

        query = amo_install.select().where(amo_install.c.referrer == referer)
        install = await database.fetch_one(query)

        query = settings.select().where(settings.c.integration_id == client_id)
        setting_info = await database.fetch_one(query)

        if not setting_info:
            return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="widget not found")

        # client_secret = "Xm3bf8KSyMUD0flV6dZkTp8Dx1aT21TtzhAkM9EH8ljglw4DTcfJAN2RdiJ6Jpw6"

        amo_post_json = {
            "client_id": client_id,
            "client_secret": setting_info.client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": setting_info.redirect_uri,
            # "redirect_uri": "https://app.tablecrm.com/api/v1/amo_connect"
        }

        if install:
            if install.from_widget != setting_info.id:
                install_add_info = await add_amo_install(amo_post_json, referer, platform, setting_info.id)

                if not scheduler.get_job(referer):
                    scheduler.add_job(refresh_token, trigger="interval", seconds=install_add_info["expires_in"], id=referer,
                                      args=[referer], max_instances=1)

                await add_job_compare(referer, install_add_info["amo_install_id"], setting_info)
            else:
                if not install["active"]:
                    amo_db_data = await update_amo_install(amo_post_json, referer, install, code)

                    if not scheduler.get_job(referer):
                        scheduler.add_job(refresh_token, trigger="interval", seconds=amo_db_data["expires_in"],
                                          id=referer,
                                          args=[referer])

                    await add_job_compare(referer, install.id, setting_info)
        else:
            install_add_info = await add_amo_install(amo_post_json, referer, platform, setting_info.id)
            scheduler.add_job(refresh_token, trigger="interval", seconds=install_add_info["expires_in"], id=referer,
                              args=[referer])
            await add_job_compare(referer, install_add_info["amo_install_id"], setting_info)




@router.get("/amo_disconnect")
async def sc_l(account_id: int, client_uuid: str):
    user = True
    if user:

        query = amo_install.select().where(
            amo_install.c.amo_account_id == account_id and amo_install.c.client_id == client_uuid)
        a_t = await database.fetch_one(query)

        if a_t:
            db_dict = dict(a_t)
            db_dict["active"] = False
            db_dict["updated_at"] = int(datetime.utcnow().timestamp())

            query = amo_install.update().where(
                amo_install.c.amo_account_id == account_id and amo_install.c.client_id == client_uuid).values(db_dict)
            await database.execute(query)

            integration_dict = {"status": False, "updated_at": int(datetime.utcnow().timestamp())}

            query = amo_install_table_cashboxes.update().where(
                amo_install_table_cashboxes.c.amo_integration_id == a_t["id"]).values(integration_dict)
            await database.execute(query)

            query = amo_install_table_cashboxes.select().where(
                amo_install_table_cashboxes.c.amo_integration_id == a_t["id"])
            relship = await database.fetch_one(query)

            if relship:
                query = users_cboxes_relation.select().where(users_cboxes_relation.c.cashbox_id == relship["cashbox_id"])
                cashboxes = await database.fetch_all(query)

                for cashbox in cashboxes:
                    await manager.send_message(cashbox.token,
                                               {"action": "paired", "target": "integrations", "integration_status": False})

            if scheduler.get_job(db_dict["referrer"]):
                scheduler.remove_job(db_dict["referrer"])

            if scheduler.get_job(f"compare_contacts_{db_dict['referrer']}"):
                scheduler.remove_job(f"compare_contacts_{db_dict['referrer']}")

            return {"status": "amo token disconnected succesfully!"}
        else:
            return {"result": "amo token does not connected!"}

    else:
        return {"status": "incorrect token!"}

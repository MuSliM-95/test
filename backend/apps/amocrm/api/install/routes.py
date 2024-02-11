from datetime import datetime

import aiohttp
from fastapi import HTTPException, APIRouter
from starlette import status


from apps.amocrm.function import refresh_token, add_job_compare
from apps.amocrm.sdk.oauth import AmoCRMAuthenticator
from database.db import amo_install, database, amo_settings, amo_install_table_cashboxes, users_cboxes_relation, \
    amo_install_settings, amo_settings_load_types
from functions.helpers import gen_token
from jobs import scheduler
from ws_manager import manager

router = APIRouter(tags=["amocrm"])


@router.get("/amo_connect")
async def sc_l(code: str, referer: str, platform: int, client_id: str, from_widget: str):
    query = amo_install.select().where(amo_install.c.referrer == referer)
    install = await database.fetch_one(query)

    query = amo_settings.select().where(amo_settings.c.integration_id == client_id)
    setting_info = await database.fetch_one(query)

    if not setting_info:
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="widget not found")

    async with aiohttp.ClientSession() as session:

        install_active = False if not install else install["active"]
        if not install_active:
            amocrm_auth = AmoCRMAuthenticator(session, client_id, setting_info.client_secret,
                                              setting_info.redirect_uri, referer)
            amo_crm_install = await amocrm_auth.authenticate(code)

            field_id = await amo_crm_install.get_custom_contact_phone_field()
            account_info = await amo_crm_install.get_account_info()
            if install and not install_active:
                query = amo_install.update().where(amo_install.c.referrer == referer)
            else:
                query = amo_install.insert().returning(amo_install.c.id)
            query = query.values(
                code=code,
                referrer=referer,
                platform=platform,
                amo_account_id=account_info["id"],
                client_id=client_id,
                client_secret=setting_info.client_secret,
                refresh_token=amo_crm_install.refresh_token,
                access_token=amo_crm_install.access_token,
                pair_token=gen_token(),
                expires_in=int(amo_crm_install.expires_in),
                field_id=field_id,
                active=True
            )
            if install and not install_active:
                await database.execute(query)
                install_id = install.id
            else:
                query = query.returning(amo_install.c.id)
                amo_install_record = await database.fetch_one(query)
                install_id = amo_install_record.id

            if not scheduler.get_job(referer):
                scheduler.add_job(refresh_token, trigger="interval", seconds=int(amo_crm_install.expires_in),
                                  id=referer,
                                  args=[referer], max_instances=1)
        else:
            install_id = install.id

        query = amo_install_settings.select().where(amo_install_settings.c.amo_install_id == install_id)
        amo_install_setting = await database.fetch_one(query)

        query = amo_settings_load_types.select().where(amo_settings_load_types.c.id == setting_info.load_type_id)
        amo_setting_load_type = await database.fetch_one(query)

        amo_setting_load_type_dict = dict(amo_setting_load_type)
        amo_setting_load_type_dict.pop("id")

        amo_install_settings_dict_values = {}

        for key in amo_setting_load_type_dict:
            if amo_setting_load_type_dict.get(key):
                amo_install_settings_dict_values[key] = amo_setting_load_type_dict.get(key)

        if amo_install_setting:
            query = amo_install_settings.update().where(amo_install_settings.c.amo_install_id == install_id)
        else:
            query = amo_install_settings.insert()

        query = query.values(amo_install_settings_dict_values)
        await database.execute(query)

        await add_job_compare(referer, install_id, setting_info.load_type_id)


@router.get("/amo_disconnect")
async def sc_l(account_id: int, client_uuid: str):
    query = amo_install.select().where(
        amo_install.c.amo_account_id == account_id and amo_install.c.client_id == client_uuid)
    a_t = await database.fetch_one(query)

    if not a_t:
        return {"result": "amo token does not connected!"}

    query = amo_install_settings.select().where(amo_install_settings.c.amo_install_id == a_t.id)
    amo_install_setting = await database.fetch_one(query)

    if not amo_install_setting:
        return {"result": "amo token does not connected!"}

    amo_install_setting_dict = dict(amo_install_setting)
    amo_install_setting_dict.pop("id")
    amo_install_setting_dict.pop("amo_install_id")

    flag = True

    for key in amo_install_setting_dict:
        if amo_install_setting_dict.get(key):  # Если какая-то из выкачек включена
            flag = False

    if flag:

        db_dict = {"active": False, "updated_at": int(datetime.utcnow().timestamp())}

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
            query = users_cboxes_relation.select().where(
                users_cboxes_relation.c.cashbox_id == relship["cashbox_id"])
            cashboxes = await database.fetch_all(query)

            for cashbox in cashboxes:
                await manager.send_message(cashbox.token,
                                           {"action": "paired", "target": "integrations",
                                            "integration_status": False})

        if scheduler.get_job(db_dict["referrer"]):
            scheduler.remove_job(db_dict["referrer"])

        if scheduler.get_job(f"compare_contacts_{db_dict['referrer']}"):
            scheduler.remove_job(f"compare_contacts_{db_dict['referrer']}")

        return {"status": "amo token disconnected succesfully!"}

    else:

        query = amo_settings.select().where(amo_settings.c.integration_id == client_uuid)
        setting_info = await database.fetch_one(query)

        query = amo_settings_load_types.select().where(amo_settings_load_types.c.id == setting_info.load_type_id)
        amo_setting_load_type = await database.fetch_one(query)

        amo_setting_load_type_dict = dict(amo_setting_load_type)
        amo_setting_load_type_dict.pop("id")

        amo_install_settings_dict_values = {}

        for key in amo_setting_load_type_dict:
            if amo_setting_load_type_dict.get(key):
                amo_install_settings_dict_values[key] = not amo_setting_load_type_dict.get(key)

        query = (
            amo_install_settings.update()
            .where(amo_install_settings.c.amo_install_id == a_t.id)
            .values(amo_install_settings_dict_values))
        await database.execute(query)

        await add_job_compare(a_t.referrer, a_t.id, setting_info.load_type_id)

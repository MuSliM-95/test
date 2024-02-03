import asyncio
from datetime import timedelta, datetime

import aiohttp
from sqlalchemy import select, desc, or_

from apps.amocrm.tasks.function import phone_normalizer
from database.db import amo_install, database, amo_contacts, amo_install_table_cashboxes, amo_table_contacts, \
    contragents


async def compare_contacts(amo_install_id: int):
    await load_amo_contacts(amo_install_id)
    await compare_amo_to_table(amo_install_id)


async def compare_amo_to_table(amo_install_id: int):
    query = (
        select([amo_install.c.access_token, amo_install.c.referrer])
        .where(amo_install.c.id == amo_install_id)
    )
    amo_install_info = await database.fetch_one(query)

    if not amo_install_info:
        return {
            "status": "error",
            "detail": "amo install not found"
        }

    query = (
        select([amo_install_table_cashboxes.c.cashbox_id])
        .where(amo_install_table_cashboxes.c.amo_integration_id == amo_install_id)
        .where(amo_install_table_cashboxes.c.status == True)
    )
    amo_table_link = await database.fetch_one(query)

    if not amo_table_link:
        return {
            "status": "error",
            "detail": "amo dont connect to table"
        }

    query = (
        amo_contacts.select()
        .where(amo_contacts.c.amo_install_id == amo_install_id)
        .where(amo_contacts.c.is_active == True)
    )
    contacts = await database.fetch_all(query)

    contacts_phone_new = []

    for contact_info in contacts:
        if contact_info.formatted_phone in contacts_phone_new:
            query = (
                amo_contacts.update()
                .where(amo_contacts.c.id == contact_info.id)
                .values({
                    "is_active": False
                })
            )
            await database.execute(query)
            continue

        query = (
            amo_table_contacts.select()
            .where(amo_table_contacts.c.amo_id == contact_info.id)
            .where(amo_table_contacts.c.amo_install_id == amo_install_id)
            .where(amo_table_contacts.c.cashbox_id == amo_table_link.cashbox_id)
        )
        table_contact_exist = await database.fetch_one(query)

        if table_contact_exist:
            query = (
                contragents.select()
                .where(contragents.c.id == table_contact_exist.table_id)
            )
            contragent_info = await database.fetch_one(query)

            body = {}
            if contragent_info.name != contact_info.name:
                body["name"] = contact_info.name
            if body:
                query = (
                    contragents.update()
                    .where(contragents.c.id == amo_table_contacts.table_id)
                    .values(body)
                )
                await database.execute(query)

        else:
            contragent_info = None
            if contact_info.formatted_phone:
                contacts_phone_new.append(contact_info.formatted_phone)

                query = (
                    contragents.select()
                    .where(contragents.c.cashbox == amo_table_link.cashbox_id)
                    .where(or_(contragents.c.phone == contact_info.formatted_phone,
                               contragents.c.phone == contact_info.phone))
                )
                contragent_info = await database.fetch_one(query)

            if contragent_info:
                query = amo_table_contacts.insert().values({
                    "amo_id": contact_info.id,
                    "table_id": contragent_info.id,
                    "cashbox_id": amo_table_link.cashbox_id,
                    "amo_install_id": amo_install_id
                })
                await database.execute(query)
            else:
                query = (
                    contragents.insert()
                    .values({
                        "name": contact_info.name,
                        "phone": contact_info.formatted_phone,
                        "cashbox": amo_table_link.cashbox_id,
                        "is_deleted": False,
                        "created_at": int(datetime.utcnow().timestamp()),
                        "updated_at": int(datetime.utcnow().timestamp()),
                    }).returning(contragents.c.id)
                )
                contragents_record = await database.fetch_one(query)

                query = amo_table_contacts.insert().values({
                    "amo_id": contact_info.id,
                    "table_id": contragents_record.id,
                    "cashbox_id": amo_table_link.cashbox_id,
                    "amo_install_id": amo_install_id
                })
                await database.execute(query)


async def load_amo_contacts(amo_install_id: int):
    query = (
        select([amo_install.c.access_token, amo_install.c.referrer])
        .where(amo_install.c.id == amo_install_id)
    )
    amo_install_info = await database.fetch_one(query)

    if not amo_install_info:
        return {
            "status": "error",
            "detail": "amo install not found"
        }

    q = (
        amo_contacts.select()
        .where(amo_contacts.c.amo_install_id == amo_install_id)
        .order_by(desc(amo_contacts.c.updated_at))
    )
    last_contact = await database.fetch_one(q)

    last_date_timestamp = None
    if last_contact:
        last_date_timestamp = (last_contact.updated_at - timedelta(seconds=15)).timestamp()

    headers = {"Authorization": f"Bearer {amo_install_info.access_token}"}
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as http_session:
        page = 1
        while True:
            amo_contacts_list = []
            try:
                url = f"https://{amo_install_info.referrer}/api/v4/contacts?page={page}&limit=250&order[updated_at]=asc"
                if last_date_timestamp:
                    url += f"&filter[updated_at][from]={last_date_timestamp}"
                print(f"Fetching amo_install {amo_install_id} contacts page {page}\n    url: {url}")
                async with http_session.get(url) as contact_resp:
                    if contact_resp.status == 204:
                        break
                    elif contact_resp.status == 401:
                        print(f"Failed 401 to fetch contacts referrer: {amo_install_info.referrer}")
                        break

                    else:
                        resp_json = await contact_resp.json()
                        if "_embedded" in resp_json:
                            if "contacts" in resp_json["_embedded"]:
                                for contact in resp_json["_embedded"]["contacts"]:
                                    phone = ""
                                    if "custom_fields_values" in contact and contact[
                                        "custom_fields_values"] is not None:
                                        for item in contact["custom_fields_values"]:
                                            if "field_code" in item and item["field_code"] == "PHONE":
                                                if "values" in item and len(item["values"]) > 0:
                                                    value = item["values"][0]
                                                    phone = value["value"]

                                    amo_contacts_list.append(
                                        {
                                            "name": contact["name"],
                                            "phone": phone,
                                            "formatted_phone": await phone_normalizer(phone),
                                            "amo_install_id": amo_install_id,
                                            "ext_id": contact["id"],
                                        }
                                    )
                        await save_amo_contacts(amo_contacts_list)
                page += 1
                await asyncio.sleep(0.4)
            except aiohttp.client_exceptions.ServerDisconnectedError:
                continue


async def save_amo_contacts(contacts):
    insert_result = []
    # result_insert_numbers = []
    for contact in contacts:
        exist_q = (
            amo_contacts.select()
            .where(amo_contacts.c.ext_id == contact["ext_id"])
            .where(amo_contacts.c.amo_install_id == contact["amo_install_id"])
        )
        exist = await database.fetch_one(exist_q)

        if exist:
            body = {}
            if exist.name != contact["name"]:
                body['name'] = contact["name"]

            if body:
                q = (
                    amo_contacts.update()
                    .where(amo_contacts.c.ext_id == contact["ext_id"])
                    .where(amo_contacts.c.amo_install_id == contact["amo_install_id"])
                    .values(body)
                )
                await database.execute(q)
        else:
            insert_result.append(contact)
            # if contact["formatted_phone"] not in result_insert_numbers:
            #     result_insert_numbers.append(contact["formatted_phone"])

    if insert_result:
        q = amo_contacts.insert().values(insert_result)
        await database.execute(q)

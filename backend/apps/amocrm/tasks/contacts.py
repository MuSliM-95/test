import asyncio
from datetime import timedelta

import aiohttp
from sqlalchemy import select, desc

from apps.amocrm.tasks.function import phone_normalizer
from database.db import amo_install, database, amo_contacts


async def compare_contacts(amo_install_id: int):
    await load_amo_contacts(amo_install_id)


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
                url = f"https://{amo_install_info.referrer}/api/v4/contacts?page={page}&limit=250&order[id]=asc"
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
                                            "formatted_phone": phone_normalizer(phone),
                                            "amo_install_id": amo_install_id,
                                            "ext_id": contact["id"],
                                        }
                                    )
                        await save_amo_contacts(amo_contacts_list)
                page += 1
                await asyncio.sleep(1)
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

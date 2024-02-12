from datetime import timedelta, datetime

import aiohttp
from aiohttp import ClientResponseError
from sqlalchemy import select, desc, or_

from apps.amocrm.sdk.exceptions import AmoApiPageIsEmpty, AmoInstallNotFound, AmoLinkTableNotFound
from apps.amocrm.tasks.function import phone_normalizer
from database.db import amo_install, database, amo_contacts, amo_install_table_cashboxes, amo_table_contacts, \
    contragents, amo_contacts_double


async def sync_contacts(amo_install_id: int):
    """
    Функция, которая сравнивает контакты в системе и в AmoCRM
    """
    try:
        amo_install_info = await get_amo_install(amo_install_id)
        cashbox_id = await get_table_cashbox_id(amo_install_id)
    except AmoInstallNotFound:
        return
    except AmoLinkTableNotFound:
        return
    timestamp_last_contact = await get_timestamp_last_contact(amo_install_id)
    page = 1
    while True:
        try:
            amo_contacts_list = await load_amo_contacts(amo_install_info["access_token"],
                                                        amo_install_info["referrer"],
                                                        timestamp_last_contact,
                                                        page)
            print("------")
            print(amo_contacts_list)
            print("------")
            amo_contacts_list_prepared = await prepare_contacts_list(amo_contacts_list, amo_install_id)
            print("------")
            print(amo_contacts_list_prepared)
            print("------")
            exist_contacts, new_contacts = await split_contacts(amo_contacts_list_prepared)
            print("СУЩЕСТВУЮЩИЕ")
            print(exist_contacts)
            print("НОВЫЕ")
            print(new_contacts)
            print("СОХРАНЕНИЕ")
            await save_exist_contacts(exist_contacts)
            await save_new_contacts(new_contacts)
            page += 1
        except AmoApiPageIsEmpty:
            break
        except ClientResponseError as e:
            print(f"Request to AMO INSTALL №{amo_install_id} failed with status {e.status} with {e.message}")
            break

    await compare_amo_to_table(amo_install_id, cashbox_id)
    await compare_table_to_amo(amo_install_id, cashbox_id, amo_install_info["field_id"],
                               amo_install_info["access_token"],
                               amo_install_info["referrer"])


async def save_new_contacts(new_contacts):
    exist_phone_contact_list = []
    new_contacts_insert = []

    for new_contact in new_contacts:
        exist_phone_contact = None
        if new_contact["formatted_phone"]:
            query = (
                amo_contacts.select()
                .where(amo_contacts.c.formatted_phone == new_contact["formatted_phone"])
                .where(amo_contacts.c.amo_install_id == new_contact["amo_install_id"])
            )
            exist_phone_contact = await database.fetch_one(query)
        if exist_phone_contact:
            exist_phone_contact_list.append({
                "orig_id": exist_phone_contact.id,
                **new_contact
            })
        else:
            new_contacts_insert.append(new_contact)

    if exist_phone_contact_list:
        query = amo_contacts_double.insert().values(exist_phone_contact_list)
        await database.execute(query)
    if new_contacts_insert:
        query = amo_contacts.insert().values(new_contacts_insert)
        await database.execute(query)


async def save_exist_contacts(exist_contacts):
    for coming_contact, exist_contact in exist_contacts:
        body = {}
        if coming_contact["name"] != exist_contact["name"]:
            body["name"] = coming_contact["name"]
        if coming_contact["formatted_phone"]:
            if exist_contact["formatted_phone"]:
                if coming_contact["formatted_phone"] != exist_contact["formatted_phone"]:
                    body["phone"] = coming_contact["phone"]
                    body["formatted_phone"] = coming_contact["formatted_phone"]
            else:
                body["phone"] = coming_contact["phone"]
                body["formatted_phone"] = coming_contact["formatted_phone"]
        if body:
            body["updated_at"] = coming_contact["updated_at"]
            query = (
                amo_contacts.update()
                .where(amo_contacts.c.id == exist_contact["id"])
                .values(body)
            )
            await database.execute(query)


async def get_timestamp_last_contact(amo_install_id: int):
    """
    Функция, которая возвращает timestamp последнего контакта в системе
    """
    query = (
        amo_contacts.select()
        .where(amo_contacts.c.amo_install_id == amo_install_id)
        .order_by(desc(amo_contacts.c.updated_at))
    )
    last_contact = await database.fetch_one(query)

    last_date_timestamp = None
    if last_contact:
        last_date_timestamp = last_contact.updated_at - 30

    return last_date_timestamp


async def get_amo_install(amo_install_id: int):
    """
    Фунцкия получения данных об инсталляции по ID
    """
    query = (
        select([amo_install.c.access_token, amo_install.c.referrer, amo_install.c.field_id])
        .where(amo_install.c.id == amo_install_id)
    )
    amo_install_info = await database.fetch_one(query)

    if not amo_install_info:
        raise AmoInstallNotFound

    return {
        "is_success": True,
        "access_token": amo_install_info.access_token,
        "referrer": amo_install_info.referrer,
        "field_id": amo_install_info.field_id,
    }


async def get_table_cashbox_id(amo_install_id: int):
    """
    Функция получения ID table сервиса
    """
    query = (
        select([amo_install_table_cashboxes.c.cashbox_id])
        .where(amo_install_table_cashboxes.c.amo_integration_id == amo_install_id)
        .where(amo_install_table_cashboxes.c.status == True)
    )
    amo_table_link = await database.fetch_one(query)

    if not amo_table_link:
        raise AmoLinkTableNotFound

    return amo_table_link.cashbox_id


async def split_contacts(contacts_list):
    """
    Функция, которая разделяет на существующие и несуществующие контакты в системе
    """
    exist_contacts = []
    new_contacts = []

    for contact in contacts_list:
        exist_q = (
            amo_contacts.select()
            .where(amo_contacts.c.ext_id == contact["ext_id"])
            .where(amo_contacts.c.amo_install_id == contact["amo_install_id"])
        )
        exist = await database.fetch_one(exist_q)

        if exist:
            exist_contacts.append((contact, dict(exist)))
        else:
            query_q_2 = (
                amo_contacts_double.select()
                .where(amo_contacts.c.ext_id == contact["ext_id"])
                .where(amo_contacts.c.amo_install_id == contact["amo_install_id"])
            )
            exist_2 = await database.fetch_one(query_q_2)

            if not exist_2:
                new_contacts.append(contact)

    return exist_contacts, new_contacts


async def load_amo_contacts(access_token: str, referrer: str, last_date_timestamp: int, page: int):
    """
    Функция обращения к AmoCRM API для получения контактов по определённое страничке,
    а также выбрасывания исключения для последующей обработки
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as http_session:
        url = f"https://{referrer}/api/v4/contacts?page={page}&limit=250&order[updated_at]=asc"
        if last_date_timestamp:
            url += f"&filter[updated_at][from]={last_date_timestamp}"
        async with http_session.get(url) as contact_resp:
            contact_resp.raise_for_status()
            if contact_resp.status == 200:
                resp_json = await contact_resp.json()
                if "next" not in resp_json.get("_links", []):
                    raise AmoApiPageIsEmpty
                if "_embedded" in resp_json:
                    if "contacts" in resp_json["_embedded"]:
                        return resp_json["_embedded"]["contacts"]
                    else:
                        print(f"Failed to fetch contacts referrer: {referrer}, empty response")
                else:
                    print(f"Failed to fetch contacts referrer: {referrer}, empty response")
                    return []


async def prepare_contacts_list(contacts_list, amo_install_id):
    """
    Функция, которая подготавливает список контактов для последующего сохранения в БД
    """
    amo_contacts_list = []
    for contact in contacts_list:
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
                "created_at": contact["created_at"],
                "updated_at": contact["updated_at"],
            }
        )
    return amo_contacts_list


async def compare_table_to_amo(amo_install_id: int, cashbox_id: int, field_id, access_token, referrer):
    query = (
        contragents.select()
        .where(contragents.c.cashbox == cashbox_id)
    )
    contragents_list = await database.fetch_all(query)

    patch_body = []
    post_body = []
    post_add_body = []
    counter = 0

    for contragent_info in contragents_list:
        query = (
            amo_table_contacts.select()
            .where(amo_table_contacts.c.table_id == contragent_info.id)
            .where(amo_table_contacts.c.cashbox_id == cashbox_id)
            .where(amo_table_contacts.c.amo_install_id == amo_install_id)
        )
        amo_table_contact_link = await database.fetch_one(query)

        if amo_table_contact_link:
            query = (
                amo_contacts.select()
                .where(amo_contacts.c.id == amo_table_contact_link.amo_id)
            )
            amo_contact = await database.fetch_one(query)

            if amo_contact:
                if amo_contact.name != contragent_info.name:
                    patch_body.append(
                        {
                            "id": amo_contact.ext_id,
                            "name": contragent_info.name
                        }
                    )
        else:
            post_dict = {"name": "Без имени" if not contragent_info.name else contragent_info.name}
            if contragent_info.phone:
                if field_id:
                    post_dict["custom_fields_values"] = [
                        {
                            "field_code": 'PHONE',
                            "field_id": field_id,
                            "values": [
                                {
                                    "value": await phone_normalizer(contragent_info.phone)
                                }
                            ]
                        }
                    ]
            post_dict["request_id"] = str(counter)
            post_body.append(post_dict)
            post_add_body_dict = {
                "table_id": contragent_info.id,
                "name": contragent_info.name,
            }
            if contragent_info.phone:
                if field_id:
                    post_add_body_dict["phone"] = await phone_normalizer(contragent_info.phone)
            post_add_body.append(post_add_body_dict)
            counter += 1

        headers = {"Content-type": "application/json", "Authorization": f"Bearer {access_token}"}
        url = f"https://{referrer}/api/v4/contacts"

        async with aiohttp.ClientSession(headers=headers) as http_session:
            if patch_body:
                for i in range(0, len(patch_body), 250):
                    try:
                        async with http_session.patch(url, json=patch_body[i:i + 250]) as patch_resp:
                            patch_resp.raise_for_status()
                    except aiohttp.ClientResponseError as e:
                        print(f"{e.status} - {e.message}")
            if post_body:
                for i in range(0, len(post_body), 250):
                    try:
                        async with http_session.post(url, json=post_body[i:i + 250]) as post_resp:
                            post_resp.raise_for_status()
                            if post_resp.status == 200:
                                post_resp_json = await post_resp.json()
                                for contact in post_resp_json['_embedded']['contacts']:
                                    request_id = contact["request_id"]

                                    amo_add_info = post_add_body[int(request_id)]

                                    table_id = amo_add_info['table_id']

                                    create_body = {
                                        "name": amo_add_info["name"],
                                        "phone": amo_add_info.get("phone"),
                                        "formatted_phone": amo_add_info.get("phone"),
                                        "ext_id": contact['id'],
                                        "amo_install_id": amo_install_id
                                    }

                                    query = (
                                        amo_contacts.insert().values(create_body).returning(amo_contacts.c.id)
                                    )
                                    created_id = await database.fetch_one(query)

                                    query = (
                                        amo_table_contacts.insert()
                                        .values(
                                            {
                                                "amo_id": created_id.id,
                                                "table_id": table_id,
                                                "cashbox_id": cashbox_id,
                                                "amo_install_id": amo_install_id
                                            }
                                        )
                                    )
                                    await database.execute(query)

                    except aiohttp.ClientResponseError as e:
                        print(f"{e.status} - {e.message}")


async def compare_amo_to_table(amo_install_id: int, cashbox_id: int):
    query = (
        amo_contacts.select()
        .where(amo_contacts.c.amo_install_id == amo_install_id)
    )
    contacts = await database.fetch_all(query)

    for contact_info in contacts:
        query = (
            amo_table_contacts.select()
            .where(amo_table_contacts.c.amo_id == contact_info.id)
            .where(amo_table_contacts.c.amo_install_id == amo_install_id)
            .where(amo_table_contacts.c.cashbox_id == cashbox_id)
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
                if contact_info.updated_at >= contragent_info.updated_at:
                    body["name"] = contact_info.name

            if contragent_info.phone != contact_info.phone:
                if contact_info.updated_at >= contragent_info.updated_at:
                    body["phone"] = await phone_normalizer(contact_info.phone)

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
                query = (
                    contragents.select()
                    .where(contragents.c.cashbox == cashbox_id)
                    .where(or_(contragents.c.phone == contact_info.formatted_phone,
                               contragents.c.phone == contact_info.phone))
                )
                contragent_info = await database.fetch_one(query)

            if contragent_info:
                query = amo_table_contacts.insert().values({
                    "amo_id": contact_info.id,
                    "table_id": contragent_info.id,
                    "cashbox_id": cashbox_id,
                    "amo_install_id": amo_install_id
                })
                await database.execute(query)
            else:
                query = (
                    contragents.insert()
                    .values({
                        "name": contact_info.name,
                        "phone": contact_info.formatted_phone,
                        "cashbox": cashbox_id,
                        "is_deleted": False,
                        "created_at": int(datetime.utcnow().timestamp()),
                        "updated_at": int(datetime.utcnow().timestamp()),
                    }).returning(contragents.c.id)
                )
                contragents_record = await database.fetch_one(query)

                query = amo_table_contacts.insert().values({
                    "amo_id": contact_info.id,
                    "table_id": contragents_record.id,
                    "cashbox_id": cashbox_id,
                    "amo_install_id": amo_install_id
                })
                await database.execute(query)

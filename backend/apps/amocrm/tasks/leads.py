import asyncio
from datetime import datetime

import aiohttp
from aiohttp import ClientResponseError
from sqlalchemy import desc

from apps.amocrm.sdk.exceptions import AmoInstallNotFound, AmoLinkTableNotFound, AmoApiPageIsEmpty, AmoUnAuthorized
from apps.amocrm.tasks.contacts import get_amo_install, get_table_cashbox_id
from database.db import database, amo_install, amo_lead_pipelines, amo_lead_statuses, amo_leads, amo_contacts, \
    amo_contacts_double


async def sync_leads(amo_install_id: int):
    """
        Функция, которая сравнивает лиды в системе и в AmoCRM
    """
    try:
        amo_install_info = await get_amo_install(amo_install_id)
        cashbox_id = await get_table_cashbox_id(amo_install_id)
    except AmoInstallNotFound:
        return
    except AmoLinkTableNotFound:
        return
    try:
        amo_pipelines_list, amo_statuses_list = await load_amo_pipelines_statuses(amo_install_id,
                                                                                  amo_install_info["referrer"],
                                                                                  amo_install_info["access_token"])
        await save_amo_pipelines_statuses(amo_pipelines_list, amo_statuses_list)
    except AmoApiPageIsEmpty:
        return
    except ClientResponseError as e:
        print(f"Request to load leads AMO INSTALL №{amo_install_id} failed with status {e.status} with {e.message}")
        return

    page = 1
    while True:
        try:
            amo_leads_list = await load_amo_leads(amo_install_id, amo_install_info["referrer"], page,
                                                  amo_install_info["access_token"])
            await check_amo_lead_base(amo_leads_list)
            page += 1
        except AmoApiPageIsEmpty:
            break
        except ClientResponseError as e:
            print(f"Request to load leads AMO INSTALL №{amo_install_id} failed with status {e.status} with {e.message}")
            return


async def load_amo_pipelines_statuses(amo_install_id, referrer, access_token):
    amo_pipelines_list = []
    amo_statuses_list = []
    headers = {"Authorization": f"Bearer {access_token}"}
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as http_session:
        url = f"https://{referrer}/api/v4/leads/pipelines"
        async with http_session.get(url) as pipelines_resp:
            pipelines_resp.raise_for_status()
            if pipelines_resp.status == 204:
                raise AmoApiPageIsEmpty
            if pipelines_resp.status == 200:
                resp_json = await pipelines_resp.json()
                if "_embedded" in resp_json:
                    if "pipelines" in resp_json["_embedded"]:
                        for pipeline in resp_json["_embedded"]["pipelines"]:

                            amo_pipelines_list.append(
                                {
                                    "name": pipeline["name"],
                                    "amo_install_id": amo_install_id,
                                    "amo_id": pipeline["id"],
                                    "sort": pipeline['sort'],
                                    "is_main": pipeline["is_main"],
                                    "is_unsorted_on": pipeline["is_unsorted_on"],
                                    "is_archive": pipeline["is_archive"],
                                    "account_id": pipeline["account_id"],
                                }
                            )
                            if "_embedded" in pipeline:
                                if "statuses" in pipeline["_embedded"]:
                                    for status in pipeline["_embedded"]["statuses"]:
                                        amo_statuses_list.append(
                                            {
                                                "amo_id": status["id"],
                                                "name": status["name"],
                                                "sort": status["sort"],
                                                "is_editable": status["is_editable"],
                                                "color": status["color"],
                                                "type": status["type"],
                                                "account_id": status["account_id"],
                                                "amo_install_id": amo_install_id,
                                                "pipeline_amo_id": pipeline["id"],
                                            }
                                        )
    return amo_pipelines_list, amo_statuses_list


async def save_amo_pipelines_statuses(pipelines, statuses):
    pipelines_actions = []
    for pipeline in pipelines:
        exist_q = (
            amo_lead_pipelines.select()
            .where(amo_lead_pipelines.c.amo_id == pipeline["amo_id"])
            .where(amo_lead_pipelines.c.amo_install_id == pipeline["amo_install_id"])
        )
        exist = await database.fetch_one(exist_q)

        q = None

        if exist:
            body = {}
            flag = False

            if exist.name != pipeline["name"]:
                body['name'] = pipeline["name"]
                flag = True
            if exist.is_main != pipeline["is_main"]:
                body['is_main'] = pipeline["is_main"]
                flag = True
            if exist.is_unsorted_on != pipeline["is_unsorted_on"]:
                body['is_unsorted_on'] = pipeline["is_unsorted_on"]
                flag = True
            if exist.is_archive != pipeline["is_archive"]:
                body['is_archive'] = pipeline["is_archive"]
                flag = True
            if exist.sort != pipeline["sort"]:
                body['sort'] = pipeline["sort"]
                flag = True
            if flag:
                q = amo_lead_pipelines.update().where(amo_lead_pipelines.c.amo_id == pipeline["amo_id"]) \
                    .where(amo_lead_pipelines.c.amo_install_id == pipeline["amo_install_id"]) \
                    .values(body)
        else:
            q = amo_lead_pipelines.insert().values(pipeline)
            pipelines_actions.append({"pipeline": pipeline, "action": "create"})

        if q is not None:
            await database.execute(q)

        for status in statuses:
            pipeline_q = amo_lead_pipelines.select() \
                .where(amo_lead_pipelines.c.amo_id == status["pipeline_amo_id"]) \
                .where(amo_lead_pipelines.c.amo_install_id == status["amo_install_id"])
            pipeline_found = await database.fetch_one(pipeline_q)

            if pipeline_found:
                exist_q = amo_lead_statuses.select() \
                    .where(amo_lead_statuses.c.pipeline_id == pipeline_found.id) \
                    .where(amo_lead_statuses.c.amo_id == status["amo_id"])
                exist = await database.fetch_one(exist_q)

                q = None

                if exist:
                    body = {}
                    flag = False
                    if exist.name != status["name"]:
                        body['name'] = status["name"]
                        flag = True
                    if exist.is_editable != status["is_editable"]:
                        body['is_editable'] = status["is_editable"]
                        flag = True
                    if exist.color != status["color"]:
                        body['color'] = status["color"]
                        flag = True
                    if exist.type != status["type"]:
                        body['type'] = status["type"]
                        flag = True
                    if exist.sort != status["sort"]:
                        body['sort'] = status["sort"]
                        flag = True

                    if flag:
                        q = amo_lead_statuses.update().where(amo_lead_statuses.c.id == exist.id) \
                            .values(body)
                else:
                    status["pipeline_id"] = pipeline_found.id
                    del status["amo_install_id"]
                    del status["pipeline_amo_id"]
                    q = amo_lead_statuses.insert().values(status)

                if q is not None:
                    await database.execute(q)


async def load_amo_leads(amo_install_id, referrer, page, access_token):
    q = (amo_leads.select()
         .where(amo_leads.c.amo_install_id == amo_install_id)
         .order_by(desc(amo_leads.c.updated_at))
         )
    last_lead = await database.fetch_one(q)

    last_date_timestamp = None

    if last_lead:
        last_date_timestamp = last_lead.updated_at - 12000

    amo_leads_list = []
    headers = {"Authorization": f"Bearer {access_token}"}
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as http_session:

        print(f"Fetching client {referrer} leads page {page}")
        url = f"https://{referrer}/api/v4/leads?with=contacts&page={page}&limit=250&order[updated_at]=asc"
        if last_date_timestamp:
            url += f"&filter[updated_at][from]={int(last_date_timestamp)}"
        async with http_session.get(url) as lead_resp:
            if lead_resp.status == 204:
                raise AmoApiPageIsEmpty
            else:
                resp_json = await lead_resp.json()
                if "_embedded" in resp_json:
                    if "leads" in resp_json["_embedded"]:
                        for lead in resp_json["_embedded"]["leads"]:
                            contact_amo_id = None
                            if len(lead["_embedded"]["contacts"]) > 0:
                                contact_amo_id = lead['_embedded']['contacts'][0]['id']
                            amo_leads_list.append(
                                {
                                    "amo_id": lead["id"],
                                    "name": lead["name"],
                                    "price": lead["price"],
                                    "status_id": lead["status_id"],
                                    "pipeline_id": lead["pipeline_id"],
                                    "closed_at": None if lead["closed_at"] is None else datetime.fromtimestamp(
                                        int(lead["closed_at"])),
                                    "is_deleted": lead["is_deleted"],
                                    "score": lead["score"],
                                    "account_id": lead["account_id"],
                                    "labor_cost": lead["labor_cost"],
                                    "contact_amo_id": contact_amo_id,
                                    "amo_install_id": amo_install_id,
                                    "updated_at": int(lead["updated_at"])
                                }
                            )
    return amo_leads_list


async def check_amo_lead_base(leads):
    result_insert = []
    for lead in leads:
        exist_q = amo_leads.select() \
            .where(amo_leads.c.amo_id == lead["amo_id"]) \
            .where(amo_leads.c.amo_install_id == lead["amo_install_id"])
        exist = await database.fetch_one(exist_q)

        contact_id = None
        if lead["contact_amo_id"]:
            contact_q = amo_contacts.select() \
                .where(amo_contacts.c.ext_id == lead["contact_amo_id"]) \
                .where(amo_contacts.c.amo_install_id == lead['amo_install_id'])
            contact = await database.fetch_one(contact_q)
            if contact:
                contact_id = contact.id
            else:
                contact_q_2 = (
                    amo_contacts_double.select()
                    .where(amo_contacts_double.c.ext_id == lead["contact_amo_id"])
                    .where(amo_contacts_double.c.amo_install_id == lead['amo_install_id'])
                )
                contact_2 = await database.fetch_one(contact_q_2)
                if contact:
                    contact_id = contact_2.id
        del lead["contact_amo_id"]
        lead["contact_id"] = contact_id

        pipeline_id = None
        status_id = None
        if lead["pipeline_id"]:
            pipeline_q = amo_lead_pipelines.select() \
                .where(amo_lead_pipelines.c.amo_id == lead["pipeline_id"]) \
                .where(amo_lead_pipelines.c.amo_install_id == lead['amo_install_id'])
            pipeline = await database.fetch_one(pipeline_q)
            if pipeline:
                pipeline_id = pipeline.id

                if lead["status_id"]:
                    status_q = amo_lead_statuses.select() \
                        .where(amo_lead_statuses.c.pipeline_id == pipeline_id) \
                        .where(amo_lead_statuses.c.amo_id == lead['status_id'])
                    status = await database.fetch_one(status_q)
                    if status:
                        status_id = status.id
        lead["pipeline_id"] = pipeline_id
        lead["status_id"] = status_id
        q = None

        if exist:
            body = {}
            flag = False
            if exist.name != lead["name"]:
                body['name'] = lead["name"]
                flag = True
            if exist.price != lead["price"]:
                body['price'] = lead["price"]
                flag = True
            if exist.score != lead["score"]:
                body['score'] = lead["score"]
                flag = True
            if exist.status_id != lead["status_id"]:
                if lead["status_id"] != 48:
                    body['status_id'] = lead["status_id"]
                    flag = True
            if exist.pipeline_id != lead["pipeline_id"]:
                if lead["status_id"] != 48:
                    body['pipeline_id'] = lead["pipeline_id"]
                    flag = True
            if exist.closed_at != lead["closed_at"]:
                body['closed_at'] = lead["closed_at"]
                flag = True
            if exist.is_deleted != lead["is_deleted"]:
                body['is_deleted'] = lead["is_deleted"]
                flag = True
            if exist.labor_cost != lead["labor_cost"]:
                body['labor_cost'] = lead["labor_cost"]
                flag = True
            if exist.contact_id != lead["contact_id"]:
                body['contact_id'] = lead["contact_id"]
                flag = True
            if flag:
                q = amo_leads.update().where(amo_leads.c.amo_id == lead["amo_id"]) \
                    .where(amo_leads.c.amo_install_id == lead["amo_install_id"]) \
                    .values(body)
                await database.execute(q)
        else:
            result_insert.append(lead)
    if result_insert:
        q = amo_leads.insert().values(result_insert)
        await database.execute(q)

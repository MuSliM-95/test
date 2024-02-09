import asyncio

import aiohttp

from database.db import database, amo_install, amo_lead_pipelines, amo_lead_statuses


async def compare_leads(amo_install_id: int):
    await load_amo_pipelines_statuses(amo_install_id)
    await load_amo_leads(amo_install_id)
    # await compare_amo_to_table(amo_install_id)
    # await compare_table_to_amo(amo_install_id)


async def load_amo_pipelines_statuses(amo_install_id):
    query = amo_install.select().where(amo_install.c.id == amo_install_id)
    amo_install_info = await database.fetch_one(query)

    amo_pipelines_list = []
    amo_statuses_list = []
    headers = {"Authorization": f"Bearer {amo_install.access_token}"}
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as http_session:
        print(f"Fetching client {amo_install_id} pipelines")
        url = f"https://{amo_install_info.referrer}/api/v4/leads/pipelines"
        async with http_session.get(url) as pipelines_resp:
            await asyncio.sleep(1)
            text_resp = await pipelines_resp.text()
            logs_body = {
                "url": url,
                "amo_install_id": amo_install.id,
                "scope": "amocrm",
                "method": pipelines_resp.method,
                "status_code": pipelines_resp.status,
                "request": {},
                "response": text_resp
            }
            if pipelines_resp.status == 401:
                return False
            else:
                resp_json = await pipelines_resp.json()
                logs_body['response'] = resp_json
                if "_embedded" in resp_json:
                    if "pipelines" in resp_json["_embedded"]:
                        for pipeline in resp_json["_embedded"]["pipelines"]:

                            amo_pipelines_list.append(
                                {
                                    "name": pipeline["name"],
                                    "amo_install_id": amo_install.id,
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
                                                "amo_install_id": amo_install.id,
                                                "pipeline_amo_id": pipeline["id"],
                                            }
                                        )
    res = await save_amo_pipelines_statuses(amo_pipelines_list, amo_statuses_list)
    return res


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


async def load_amo_leads(amo_install_id):
    pass

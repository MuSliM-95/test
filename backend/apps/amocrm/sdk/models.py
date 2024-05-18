import json

import aiohttp


class AmoCRMAuthenticationResult:
    def __init__(self, access_token: str, refresh_token: str, amo_domain: str, expires_in: int):
        self.access_token = access_token
        self.expires_in = expires_in
        self.amo_domain = amo_domain
        self.refresh_token = refresh_token

    async def get_custom_contact_phone_field(self) -> int:

        field_id = None
        custom_fields_url = f'https://{self.amo_domain}/api/v4/contacts/custom_fields'
        headers = {'Authorization': f'Bearer {self.access_token}'}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(custom_fields_url) as response:
                data = await response.json()
                if data.get("_embedded"):
                    _emb = data.get("_embedded")
                    if _emb.get("custom_fields"):
                        for custom_field in data["_embedded"]["custom_fields"]:
                            if custom_field["name"] == "Телефон":
                                field_id = int(custom_field["id"])
                return field_id

    async def get_account_info(self) -> dict:
        account_info_url = f'https://{self.amo_domain}/api/v4/account'
        headers = {'Authorization': f'Bearer {self.access_token}'}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(account_info_url) as response:
                data = await response.json()
                return data

    async def get_amo_codes_leads(self):
        return [
            "manager", "paid_loyalty", "paid_rubles"
        ]

    async def get_amo_codes_contacts(self):
        return [
            "birth_date", "loyalty_card_number_phone",
        ]

    async def create_amo_group_contacts(self):
        headers = {'Authorization': f'Bearer {self.access_token}'}
        group_id = None

        async with aiohttp.ClientSession(headers=headers) as http_session:
            url = f"https://{self.amo_domain}/api/v4/contacts/custom_fields/groups"
            data = [
                {
                    "name": "Tablecrm",
                    "sort": 2
                }
            ]
            async with http_session.post(url, data=json.dumps(data)) as groups_resp:
                groups_resp.raise_for_status()
                resp_json = await groups_resp.json()
                if "_embedded" in resp_json:
                    if "custom_field_groups" in resp_json["_embedded"]:
                        for group in resp_json["_embedded"]["custom_field_groups"]:
                            if group["name"] == "Tablecrm":
                                group_id = group["id"]
        return group_id

    async def create_amo_group_leads(self):
        headers = {'Authorization': f'Bearer {self.access_token}'}
        group_id = None

        async with aiohttp.ClientSession(headers=headers) as http_session:
            url = f"https://{self.amo_domain}/api/v4/leads/custom_fields/groups"
            data = [
                {
                    "name": "Tablecrm",
                    "sort": 2
                }
            ]
            async with http_session.post(url, data=json.dumps(data)) as groups_resp:
                groups_resp.raise_for_status()
                resp_json = await groups_resp.json()
                if "_embedded" in resp_json:
                    if "custom_field_groups" in resp_json["_embedded"]:
                        for group in resp_json["_embedded"]["custom_field_groups"]:
                            if group["name"] == "Tablecrm":
                                group_id = group["id"]
        return group_id

    async def post_custom_fields_leads(self, fields_predata):
        field_ids = []
        headers = {'Authorization': f'Bearer {self.access_token}'}
        async with aiohttp.ClientSession(headers=headers) as http_session:
            url = f"https://{self.amo_domain}/api/v4/leads/custom_fields"
            request_json = json.dumps(fields_predata)
            async with http_session.post(url, data=request_json) as groups_resp:
                print(await groups_resp.text())
                groups_resp.raise_for_status()
                resp_json = await groups_resp.json()
                if "_embedded" in resp_json:
                    if "custom_fields" in resp_json["_embedded"]:
                        for y in resp_json["_embedded"]["custom_fields"]:
                            field_ids.append(y["id"])

    async def post_custom_fields_contacts(self, fields_predata):
        field_ids = []
        headers = {'Authorization': f'Bearer {self.access_token}'}
        async with aiohttp.ClientSession(headers=headers) as http_session:
            url = f"https://{self.amo_domain}/api/v4/contacts/custom_fields"
            request_json = json.dumps(fields_predata)
            async with http_session.post(url, data=request_json) as groups_resp:
                print(await groups_resp.text())
                groups_resp.raise_for_status()
                resp_json = await groups_resp.json()
                if "_embedded" in resp_json:
                    if "custom_fields" in resp_json["_embedded"]:
                        for y in resp_json["_embedded"]["custom_fields"]:
                            field_ids.append(y["id"])

    async def create_custom_fields_leads(self):
        amo_codes_create = await self.get_amo_codes_leads()
        amo_codes = await self.get_custom_fields_codes_leads()
        must_create_fields = []
        for code in amo_codes_create:
            if code.upper() not in amo_codes:
                must_create_fields.append(code)
        if len(must_create_fields) > 0:
            group_id = await self.create_amo_group_leads()
            if group_id:
                fields_predata = [
                    {
                        "code": "MANAGER",
                        "name": "Менеджер",
                        "type": "text",
                        "group_id": group_id
                    },
                    {
                        "code": f"PAID_LOYALTY",
                        "name": f"Оплачено бонусами",
                        "type": "numeric",
                        "group_id": group_id
                    },
                    {
                        "code": f"PAID_RUBLES",
                        "name": f"Оплачено рублями",
                        "type": "numeric",
                        "group_id": group_id
                    }
                ]
                field_ids = await self.post_custom_fields_leads(fields_predata)
                if field_ids:
                    print(f"КАСТОМНЫЕ ПОЛЯ AMO {self.amo_domain} УСПЕШНО СОЗДАНЫ")

            else:
                print(f"ПРИ СОЗДАНИИ КАСТОМНЫХ ПОЛЕЙ AMO {self.amo_domain} ПРОИЗОШЛА ОШИБКА")

    async def create_custom_fields_contacts(self):
        amo_codes_create = await self.get_amo_codes_contacts()
        amo_codes = await self.get_custom_fields_codes_contacts()
        must_create_fields = []
        for code in amo_codes_create:
            if code.upper() not in amo_codes:
                must_create_fields.append(code)
        if len(must_create_fields) > 0:
            group_id = await self.create_amo_group_contacts()
            if group_id:
                fields_predata = [
                    {
                        "code": "BIRTH_DATE",
                        "name": "Дата рождения",
                        "type": "date_time",
                        "group_id": group_id
                    },
                    {
                        "code": "LOYALTY_CARD_NUMBER_PHONE",
                        "name": "Карта лояльности",
                        "type": "numeric",
                        "group_id": group_id
                    }]
                field_ids = await self.post_custom_fields_contacts(fields_predata)
                if field_ids:
                    print(f"КАСТОМНЫЕ ПОЛЯ AMO {self.amo_domain} УСПЕШНО СОЗДАНЫ")

            else:
                print(f"ПРИ СОЗДАНИИ КАСТОМНЫХ ПОЛЕЙ AMO {self.amo_domain} ПРОИЗОШЛА ОШИБКА")

    async def get_custom_fields_codes_leads(self):
        codes = []
        custom_fields_url = f'https://{self.amo_domain}/api/v4/leads/custom_fields'
        headers = {'Authorization': f'Bearer {self.access_token}'}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(custom_fields_url) as response:
                response.raise_for_status()
                data = await response.json()
                if "_embedded" in data:
                    if "custom_fields" in data["_embedded"]:
                        codes = [x["code"] for x in data["_embedded"]["custom_fields"]]
        return codes

    async def get_custom_fields_codes_contacts(self):
        codes = []
        custom_fields_url = f'https://{self.amo_domain}/api/v4/contacts/custom_fields'
        headers = {'Authorization': f'Bearer {self.access_token}'}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(custom_fields_url) as response:
                response.raise_for_status()
                data = await response.json()
                if "_embedded" in data:
                    if "custom_fields" in data["_embedded"]:
                        codes = [x["code"] for x in data["_embedded"]["custom_fields"]]
        return codes

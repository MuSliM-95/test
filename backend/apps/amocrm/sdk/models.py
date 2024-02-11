import aiohttp


class AmoCRMAuthenticationResult:
    def __init__(self, access_token: str, refresh_token: str, amo_domain: str, expires_in: int):
        self.access_token = access_token
        self.expires_in = expires_in
        self.amo_domain = amo_domain
        self.refresh_token = refresh_token

    async def get_custom_contact_phone_field(self) -> tuple:
        field_id = None
        custom_fields_url = f'https://{self.amo_domain}/v4/contacts/custom_fields'
        headers = {'Authorization': f'Bearer {self.access_token}'}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(custom_fields_url) as response:
                print(await response.text())
                data = await response.json()
                if data.get("_embedded"):
                    _emb = data.get("_embedded")
                    if _emb.get("custom_fields"):
                        for custom_field in data["_embedded"]["custom_fields"]:
                            if custom_field["name"] == "Телефон":
                                field_id = int(custom_field["id"])
                return field_id, data.get("id")
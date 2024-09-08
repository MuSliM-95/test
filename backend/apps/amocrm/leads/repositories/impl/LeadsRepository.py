import aiohttp

from apps.amocrm.leads.repositories.core.ILeadsRepository import ILeadsRepository
from apps.amocrm.leads.repositories.models.CreateLeadModel import CreateLeadModel


class LeadsRepository(ILeadsRepository):

    def __init__(self):
        self.__base_url = "https://{}/api/v4/leads/complex"

    async def create_lead(self, access_token: str, amo_lead_model: CreateLeadModel, referrer: str):
        async with aiohttp.ClientSession(trust_env=True) as http_session:
            async with http_session.post(
                self.__base_url.format(referrer),
                json=[amo_lead_model.json().encode("utf-8")],
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Content-type': 'application/json'
            }) as resp:
                resp.raise_for_status()
                if resp.status == 200:
                    return await resp.json()

import aiohttp

from apps.amocrm.sdk.models import AmoCRMAuthenticationResult


class AmoCRMAuthenticationService:
    async def authenticate(self, code: str) -> AmoCRMAuthenticationResult:
        pass


class AmoCRMRefreshToken(AmoCRMAuthenticationService):
    def __init__(self, http_client: aiohttp.ClientSession, client_id: str, client_secret: str, redirect_uri: str,
                 amo_domain: str, refresh_token: str):
        self.http_client = http_client
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.amo_domain = amo_domain
        self.refresh_token = refresh_token

    async def authenticate(self, code: str) -> AmoCRMAuthenticationResult:
        params = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'authorization_code',
            'code': code,
            "refresh_token": self.refresh_token,
            'redirect_uri': self.redirect_uri,
        }

        async with self.http_client.post(f'https://{self.amo_domain}/oauth2/access_token',
                                         json=params) as response:
            data = await response.json()
            if 'access_token' in data:
                return AmoCRMAuthenticationResult(data['access_token'], self.amo_domain)
            else:
                raise ValueError('Authentication failed')


class AmoCRMAuthenticator(AmoCRMAuthenticationService):
    def __init__(self, http_client: aiohttp.ClientSession, client_id: str, client_secret: str, redirect_uri: str,
                 amo_domain: str):
        self.http_client = http_client
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.amo_domain = amo_domain

    async def authenticate(self, code: str) -> AmoCRMAuthenticationResult:
        params = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': self.redirect_uri,
        }

        print(f'https://{self.amo_domain}/oauth2/access_token')
        async with self.http_client.post(f'https://{self.amo_domain}/oauth2/access_token',
                                         json=params) as response:
            data = await response.json()
            if 'access_token' in data:
                return AmoCRMAuthenticationResult(data['access_token'], data['refresh_token'],
                                                  self.amo_domain, int(data['expires_in']))
            else:
                raise ValueError('Authentication failed')

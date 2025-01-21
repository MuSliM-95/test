
from apps.yookassa.repositories.core.IYookassaOauthRepository import IYookassaOauthRepository
from apps.yookassa.services.core.IOauthService import IOauthService


class OauthService(IOauthService):

    def __init__(self, oauth_repository: IYookassaOauthRepository):
        self.__oauth_repository = oauth_repository

    async def create(self, cashbox: int) -> str:
        credentials = self.__oauth_repository.get_oauth_credentials(cashbox)
        return f'https://yookassa.ru/oauth/v2/authorize?client_id={credentials.client_id}&response_type=code&state={credentials.cashbox}'

    async def revoke_token(self):
        pass


from apps.yookassa.functions.core.IGetOauthCredentialFunction import IGetOauthCredentialFunction
from apps.yookassa.models.OauthBaseModel import OauthUpdateModel, OauthModel, OauthBaseModel
from apps.yookassa.repositories.core.IYookassaOauthRepository import IYookassaOauthRepository
from apps.yookassa.repositories.core.IYookassaRequestRepository import IYookassaRequestRepository
from apps.yookassa.services.core.IOauthService import IOauthService


class OauthService(IOauthService):

    def __init__(
            self,
            oauth_repository: IYookassaOauthRepository,
            request_repository = IYookassaRequestRepository,
            get_oauth_credential_function = IGetOauthCredentialFunction
    ):

        self.__oauth_repository = oauth_repository
        self.__request_repository = request_repository
        self.__get_oauth_credential_function = get_oauth_credential_function

    async def oauth_link(self, cashbox: int) -> str:
        client_id, _ = self.__get_oauth_credential_function()
        return f'https://yookassa.ru/oauth/v2/authorize?client_id={client_id}&response_type=code&state={cashbox}'

    async def revoke_token(self):
        pass

    async def get_access_token(self, code: str, state: int):
        client_id, client_secret = self.__get_oauth_credential_function()
        try:
            res = await self.__request_repository.token(code=code, client_id = client_id, client_secret = client_secret)
            oauth = await self.__oauth_repository.get_oauth(state)
            if oauth:
                await self.__oauth_repository.update_oauth(
                    state,
                    OauthUpdateModel(access_token = res.access_token))
            else:
                await self.__oauth_repository.insert_oauth(
                    state,
                    OauthBaseModel(cashbox = state, access_token = res.access_token))
        except Exception as error:
            raise error



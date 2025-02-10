import base64

from apps.yookassa.functions.core.IGetOauthCredentialFunction import IGetOauthCredentialFunction
from apps.yookassa.models.OauthBaseModel import OauthUpdateModel, OauthModel, OauthBaseModel
from apps.yookassa.repositories.core.IYookassaOauthRepository import IYookassaOauthRepository
from apps.yookassa.repositories.core.IYookassaRequestRepository import IYookassaRequestRepository
from apps.yookassa.services.core.IOauthService import IOauthService


class OauthService(IOauthService):

    def __init__(
            self,
            oauth_repository: IYookassaOauthRepository,
            request_repository: IYookassaRequestRepository,
            get_oauth_credential_function: IGetOauthCredentialFunction
    ):

        self.__oauth_repository = oauth_repository
        self.__request_repository = request_repository
        self.__get_oauth_credential_function = get_oauth_credential_function

    async def oauth_link(self, cashbox: int, warehouse: int, token: str) -> str:

        client_id, _ = self.__get_oauth_credential_function()
        return f'https://yookassa.ru/oauth/v2/authorize?client_id={client_id}&response_type=code&state={base64.b64encode(f"{cashbox}:{warehouse}:{token}".encode("utf-8")).decode("utf-8")}'

    async def revoke_token(self, cashbox: int, warehouse: int):

        client_id, client_secret = self.__get_oauth_credential_function()
        oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)

        if not oauth:
            raise Exception("Отсутствует oauth2 по данному пользователю")

        await self.__request_repository.revoke_token(token = oauth.access_token, client_id = client_id, client_secret = client_secret)
        await self.__oauth_repository.delete_oauth(cashbox=cashbox)

    async def get_access_token(self, code: str, cashbox: int, warehouse: int):

        client_id, client_secret = self.__get_oauth_credential_function()

        try:
            res = await self.__request_repository.token(code=code, client_id = client_id, client_secret = client_secret)
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)

            if oauth:
                await self.__oauth_repository.update_oauth(
                    cashbox,
                    warehouse,
                    OauthUpdateModel(access_token = res.get("access_token"), warehouse_id = warehouse))
            else:
                await self.__oauth_repository.insert_oauth(
                    cashbox,
                    OauthBaseModel(cashbox_id = cashbox, access_token = res.get("access_token"), warehouse_id = warehouse))

        except Exception as error:
            raise error

    async def get_install_oauth_by_user(self, cashbox: int):

        install_oauth_list = await self.__oauth_repository.get_oauth_list(cashbox)

        return install_oauth_list



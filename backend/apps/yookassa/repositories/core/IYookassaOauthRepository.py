from apps.yookassa.models.OauthBaseModel import OauthBaseModel, OauthModel, OauthUpdateModel


class IYookassaOauthRepository:

    def get_oauth_credentials(self, cashbox: int):
        raise NotImplementedError

    async def update_oauth(self, cashbox: int, oauth: OauthUpdateModel) -> None:
        raise NotImplementedError

    async def insert_oauth(self, cashbox: int, oauth: OauthModel) -> None:
        raise NotImplementedError

    async def delete_oauth(self, cashbox: int) -> None:
        raise NotImplementedError

    async def get_oauth(self, cashbox: int) -> OauthBaseModel:
        raise NotImplementedError


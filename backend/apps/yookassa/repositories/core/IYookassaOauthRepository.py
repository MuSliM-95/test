from apps.yookassa.models.OauthBaseModel import OauthBaseModel, OauthModel, OauthUpdateModel


class IYookassaOauthRepository:

    async def update_oauth(self, cashbox: int, warehouse: int, oauth: OauthUpdateModel) -> None:
        raise NotImplementedError

    async def insert_oauth(self, cashbox: int, oauth: OauthModel) -> None:
        raise NotImplementedError

    async def delete_oauth(self, cashbox: int, warehouse: int) -> None:
        raise NotImplementedError

    async def get_oauth(self, cashbox: int, warehouse: int) -> OauthBaseModel:
        raise NotImplementedError


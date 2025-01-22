from fastapi.responses import RedirectResponse
from fastapi import status

from apps.yookassa.functions.core.IGetOauthCredentialFunction import IGetOauthCredentialFunction
from apps.yookassa.services.core.IOauthService import IOauthService
from functions.helpers import get_user_by_token


class CreateOauthView:

    def __init__(
            self,
            oauth_service: IOauthService,

    ):
        self.__oauth_service = oauth_service

    async def __call__(self, token: str):
        user = await get_user_by_token(token)
        create_link = await self.__oauth_service.create(user.cashbox_id)
        return create_link
        # return RedirectResponse(create_link, status_code = status.HTTP_303_SEE_OTHER)

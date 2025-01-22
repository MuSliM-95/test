from apps.yookassa.functions.core.IGetOauthCredentialFunction import IGetOauthCredentialFunction
from apps.yookassa.services.core.IOauthService import IOauthService

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder


class CallbackOauthView:

    def __init__(
            self,
            oauth_service: IOauthService,

    ):
        self.__oauth_service = oauth_service

    async def __call__(self, code: str, state: int):
        try:
            res = await self.__oauth_service.get_access_token(code = code, state = state)
            return res
        except Exception as error:
            raise HTTPException(
                status_code = 432,
                detail = f"ошибка при аторизации oauth2 в yookassa.ru: {str( error )}"
            )

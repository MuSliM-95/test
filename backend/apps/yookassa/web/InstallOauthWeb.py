from fastapi import FastAPI, status

from apps.yookassa.repositories.core.IYookassaOauthRepository import IYookassaOauthRepository
from apps.yookassa.services.impl.OauthService import OauthService
from apps.yookassa.web.view.CreateOauthView import CreateOauthView
from common.utils.ioc.ioc import ioc


class InstallYookassaOauthWeb:

    def __call__(self, app: FastAPI):
        create_oauth_view = CreateOauthView(
            oauth_service = OauthService(
                oauth_repository = ioc.get(IYookassaOauthRepository)
            )
        )

        app.add_api_route(
            path = "/yookassa/install",
            endpoint = create_oauth_view.__call__,
            methods = ["POST"],
            status_code = status.HTTP_200_OK,
            tags = ["yookassa"]
        )

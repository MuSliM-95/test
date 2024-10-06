from fastapi import FastAPI
from starlette import status

from apps.amocrm.api.installer.infrastructure.repositories.core.IWidgetInstallerRepository import \
    IWidgetInstallerRepository
from apps.amocrm.api.installer.web.view.AddWidgetInstallerInfoView import AddWidgetInstallerInfoView
from common.utils.ioc.ioc import ioc


class InstallWidgetInstallerInfoWeb:

    def __call__(
        self,
        app: FastAPI
    ):
        add_widget_installer_info_view = AddWidgetInstallerInfoView(
            widget_installer_repository=ioc.get(IWidgetInstallerRepository)
        )

        app.add_api_route(
            path="/widget_installer",
            endpoint=add_widget_installer_info_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            tags=["amocrm"]
        )
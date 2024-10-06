from apps.amocrm.api.installer.infrastructure.models.InsertWidgetInstallerInfoModel import \
    InsertWidgetInstallerInfoModel
from apps.amocrm.api.installer.infrastructure.models.ResponseInsertWidgetInstallerInfoModel import \
    ResponseInsertWidgetInstallerInfoModel


class IWidgetInstallerRepository:

    async def add_installer(self, widget_installer_data: InsertWidgetInstallerInfoModel) -> ResponseInsertWidgetInstallerInfoModel:
        raise NotImplementedError()
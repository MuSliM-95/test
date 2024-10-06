from apps.amocrm.api.installer.infrastructure.models.InsertWidgetInstallerInfoModel import \
    InsertWidgetInstallerInfoModel
from apps.amocrm.api.installer.infrastructure.models.ResponseInsertWidgetInstallerInfoModel import \
    ResponseInsertWidgetInstallerInfoModel
from apps.amocrm.api.installer.infrastructure.repositories.core.IWidgetInstallerRepository import \
    IWidgetInstallerRepository
from database.db import amo_install_widget_installer, database


class WidgetInstallerRepository(IWidgetInstallerRepository):

    async def add_installer(self, widget_installer_data: InsertWidgetInstallerInfoModel) -> ResponseInsertWidgetInstallerInfoModel:
        query = (
            amo_install_widget_installer.insert()
            .values(widget_installer_data.dict())
            .returning(amo_install_widget_installer.c.id)
        )
        amo_install_widget_installer_id = await database.fetch_one(query)
        return ResponseInsertWidgetInstallerInfoModel(
            **{
                "id": amo_install_widget_installer_id.id,
                **widget_installer_data.dict(),
            }
        )
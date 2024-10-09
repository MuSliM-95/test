from sqlalchemy import and_

from apps.amocrm.api.installer.infrastructure.models.InsertWidgetInstallerInfoModel import \
    InsertWidgetInstallerInfoModel
from apps.amocrm.api.installer.infrastructure.models.PatchWidgetInstallerInfoModel import PatchWidgetInstallerInfoModel
from apps.amocrm.api.installer.infrastructure.models.ResponseGetByIdWidgetInstallerInfoModel import \
    ResponseGetByIdWidgetInstallerInfoModel
from apps.amocrm.api.installer.infrastructure.models.ResponseInsertWidgetInstallerInfoModel import \
    ResponseInsertWidgetInstallerInfoModel
from apps.amocrm.api.installer.infrastructure.models.ResponsePatchWidgetInstallerInfoModel import \
    ResponsePatchWidgetInstallerInfoModel
from apps.amocrm.api.installer.infrastructure.repositories.core.IWidgetInstallerRepository import \
    IWidgetInstallerRepository
from database.db import amo_install_widget_installer, database


class WidgetInstallerRepository(IWidgetInstallerRepository):

    async def get_installer(self, amo_account_id: int, cashbox_id: int) -> ResponseGetByIdWidgetInstallerInfoModel:
        query = (
            amo_install_widget_installer.select()
            .where(
                amo_install_widget_installer.c.amo_account_id == amo_account_id,
                amo_install_widget_installer.c.client_cashbox == cashbox_id
            )
        )
        amo_install_widget_installer_info = await database.fetch_one(query)
        return ResponseGetByIdWidgetInstallerInfoModel(
            id=amo_install_widget_installer_info.id,
            amo_account_id=amo_install_widget_installer_info.amo_account_id,
            installed_by_role=amo_install_widget_installer_info.installed_by_role,
            client_name=amo_install_widget_installer_info.client_name,
            client_cashbox=amo_install_widget_installer_info.client_cashbox,
            client_number_phone=amo_install_widget_installer_info.client_number_phone,
            partner_name=amo_install_widget_installer_info.partner_name,
            partner_cashbox=amo_install_widget_installer_info.partner_cashbox,
            partner_number_phone=amo_install_widget_installer_info.partner_number_phone,
            client_inn=amo_install_widget_installer_info.client_inn,
        )


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

    async def patch_installer(
        self,
        amo_account_id: int,
        client_cashbox_id: int,
        widget_installer_data: PatchWidgetInstallerInfoModel
    ) -> ResponsePatchWidgetInstallerInfoModel:
        query = (
            amo_install_widget_installer.update()
            .where(and_(
                amo_install_widget_installer.c.amo_account_id == amo_account_id,
                amo_install_widget_installer.c.client_cashbox == client_cashbox_id
            ))
            .values(widget_installer_data.dict(exclude_none=True))
        )
        await database.execute(query)
        return ResponsePatchWidgetInstallerInfoModel(
            widget_installer_data.dict(exclude_none=True)
        )


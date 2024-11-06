from typing import Optional

from pydantic import BaseModel

from database.db import InstalledByRole


class InsertWidgetInstallerInfoModel(BaseModel):
    amo_account_id: int
    installed_by_role: InstalledByRole

    client_name: str
    client_cashbox: int
    client_number_phone: str

    partner_name: Optional[str]
    partner_cashbox: Optional[int]
    partner_number_phone: Optional[str]
    client_inn: Optional[str]
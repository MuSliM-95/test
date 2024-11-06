from typing import Optional

from pydantic import BaseModel

from database.db import InstalledByRole


class AddInstallerInfoModel(BaseModel):
    amo_account_id: int
    installed_by_role: InstalledByRole

    client_name: str
    client_token: str
    client_number_phone: str

    partner_name: Optional[str]
    partner_token: Optional[str]
    partner_number_phone: Optional[str]
    client_inn: Optional[str]

from typing import Optional

from pydantic import BaseModel

from common.apple_wallet_service.impl.models import PassColorConfig, Location


class WalletCardSettings(BaseModel):
    logo_text: str = ''
    description: str = ''
    barcode_message: str = 'TableCRM'

    colors: Optional[PassColorConfig] = PassColorConfig(
        backgroundColor="#3875f6",
        foregroundColor="#ffffff",
        labelColor="#ffffff"
    )

    # TODO; set default images. First of all create dir for that images
    icon_path: Optional[str] = ''
    logo_path: Optional[str] = ''
    strip_path: Optional[str] = ''

    locations: list[Location] = []

class WalletCardSettingsCreate(BaseModel):
    cashbox_id: int
    data: WalletCardSettings

class WalletCardSettingsUpdate(WalletCardSettingsCreate):
    data: dict


# print(WalletCardSettingsUpdate(cashbox_id=1, data=WalletCardSettings().dict(exclude_unset=True)))
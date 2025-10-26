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
    # Пути могут быть как локальными (начинаются с /), так и S3 ключами
    icon_path: Optional[str] = '/backend/static_files/AppleWalletIconDefault.png'
    logo_path: Optional[str] = '/backend/static_files/AppleWalletLogoDefault.png'
    strip_path: Optional[str] = '/backend/static_files/AppleWalletStripDefault.png'

    locations: list[Location] = []

class WalletCardSettingsCreate(BaseModel):
    cashbox_id: int
    data: WalletCardSettings

class WalletCardSettingsUpdate(WalletCardSettingsCreate):
    data: dict

import json
import os

from py_pkpass.models import StoreCard, Pass, BarcodeFormat, Barcode, Field
from sqlalchemy import select

from api.apple_wallet_card_settings.schemas import WalletCardSettings
from api.apple_wallet_card_settings.utils import create_default_apple_wallet_setting
from common.apple_wallet_service.IWalletPassGeneratorService import IWalletPassGeneratorService
from common.apple_wallet_service.impl.models import PassParamsModel
from database.db import loyality_cards, contragents, organizations, database, apple_wallet_card_settings


# load_dotenv()

class WalletPassGeneratorService(IWalletPassGeneratorService):
    def __init__(self):
        self.__wallet_pass = '/backend/photos'

    def _generate_pkpass(self, pass_params: PassParamsModel) -> tuple[str, str]:
        # Create a store card pass type
        card_info = StoreCard()

        balance_field = Field('H1', str(pass_params.balance), 'Баланс')
        balance_field.changeMessage = 'Ваш баланс %@'
        cashback_field = Field('H2', str(pass_params.cashback_persent) + '%', 'Бонусы')
        cashback_field.changeMessage = 'Ваш кешбек теперь %@'

        ad_field = Field('B1', pass_params.advertisement, 'Акции')
        ad_field.changeMessage = "%@"

        card_info.headerFields.append(balance_field)
        card_info.headerFields.append(cashback_field)
        card_info.backFields.append(ad_field)

        card_info.addSecondaryField('S1', pass_params.contragent_name, 'ВЛАДЕЛЕЦ КАРТЫ')
        card_info.addSecondaryField('S2', pass_params.card_number, 'НОМЕР КАРТЫ')

        # Create the Pass object with the required identifiers
        passfile = Pass(
            card_info,
            passTypeIdentifier=os.getenv('APPLE_PASS_TYPE_ID'),
            organizationName=pass_params.organization_name,
            teamIdentifier=os.getenv('APPLE_TEAM_ID')
        )

        # Set required pass information
        passfile.serialNumber = str(pass_params.card_number)
        passfile.description = pass_params.description

        # Add a barcode - all supported formats: PDF417, QR, AZTEC, CODE128
        passfile.barcode = Barcode(
            message=pass_params.barcode_message,
            format=BarcodeFormat.QR,
        )

        passfile.webServiceURL = f'https://{os.getenv("APP_URL")}/api/v1'
        passfile.authenticationToken = pass_params.auth_token

        # Optional: Set colors
        passfile.backgroundColor = pass_params.colors.backgroundColor
        passfile.foregroundColor = pass_params.colors.foregroundColor
        passfile.labelColor = pass_params.colors.labelColor

        passfile.logoText = pass_params.logo_text

        passfile.locations = [i.dict() for i in pass_params.locations]

        # Including the icon and logo is necessary for the passbook to be valid
        passfile.addFile('icon.png', open(pass_params.icon_path, 'rb'))
        passfile.addFile('icon@2x.png', open(pass_params.icon_path, 'rb'))
        passfile.addFile('icon@3x.png', open(pass_params.icon_path, 'rb'))
        passfile.addFile('logo.png', open(pass_params.logo_path, 'rb'))
        passfile.addFile('strip@2x.png', open(pass_params.strip_path, 'rb'))

        # passfile.expirationDate = pass_params.exp_date.isoformat() if pass_params.exp_date else None

        # Create and output the Passbook file (.pkpass)
        pkpass_path = f'{self.__wallet_pass}/{pass_params.card_number}.pkpass'
        password = os.getenv('PKPASS_PASSWORD')
        passfile.create(
            os.getenv('APPLE_CERTIFICATE_PATH'),
            os.getenv('APPLE_KEY_PATH'),
            os.getenv('APPLE_WWDR_PATH'),
            password,
            pkpass_path
        )

        return self.get_card_path_and_name(pass_params.card_number)

    def get_card_path_and_name(self, card_number: str) -> tuple[str, str]:
        return f'{self.__wallet_pass}/{card_number}.pkpass', f'{card_number}.pkpass'

    async def update_pass(self, card_id: int) -> tuple[str, str]:
        query = (
            select(
                loyality_cards.c.card_number,
                contragents.c.name.label("contragent_name"),
                organizations.c.short_name.label("organization_name"),
                loyality_cards.c.cashback_percent,
                loyality_cards.c.balance,
                loyality_cards.c.end_period,
                loyality_cards.c.cashbox_id,
                loyality_cards.c.apple_wallet_advertisement
            )
            .select_from(
                loyality_cards
                .join(
                    contragents,
                    contragents.c.id == loyality_cards.c.contragent_id
                )
                .join(
                    organizations,
                    organizations.c.id == loyality_cards.c.organization_id
                )
            ).where(loyality_cards.c.id == card_id)
        )
        loyality_card_db = await database.fetch_one(query)

        card_settings_query = select(apple_wallet_card_settings.c.data).where(
            apple_wallet_card_settings.c.cashbox_id == loyality_card_db.cashbox_id)
        card_settings_db = await database.fetch_one(card_settings_query)

        if card_settings_db is None:
            card_settings = await create_default_apple_wallet_setting(card_id)
        else:
            card_settings = WalletCardSettings(**json.loads(card_settings_db.data))

        path, filename = self._generate_pkpass(PassParamsModel(
            card_number=loyality_card_db.card_number,
            contragent_name=loyality_card_db.contragent_name,
            organization_name=loyality_card_db.organization_name,
            description=card_settings.description,
            barcode_message=card_settings.barcode_message,
            colors=card_settings.colors,
            icon_path=card_settings.icon_path,
            logo_path=card_settings.logo_path,
            strip_path=card_settings.strip_path,
            cashback_persent=loyality_card_db.cashback_percent,
            locations=card_settings.locations,
            logo_text=card_settings.logo_text,
            balance=loyality_card_db.balance,
            exp_date=loyality_card_db.end_period,
            advertisement=loyality_card_db.apple_wallet_advertisement
        ))

        return path, filename
# WalletPassGeneratorService().generate_pkpass(
#     PassParamsModel(
#         card_number='327492',
#         contragent_name='gool',
#         organization_name='Table',
#         description='first card',
#         barcode_message='first barcode',
#         colors=PassColorConfig(
#             backgroundColor='#000000',
#             foregroundColor='#888888',
#             labelColor='#FFFFFF'
#         ),
#         icon_path='/Users/reveek/PycharmProjects/tablecrm/photos/file_2.jpg',
#         logo_path='/Users/reveek/PycharmProjects/tablecrm/photos/file_2.jpg',
#         cashback_persent=5,
#         locations=[],
#         logo_text='logo text',
#         balance=1000,
#         strip_path='/Users/reveek/PycharmProjects/tablecrm/photos/file_2.jpg',
#     )
# )
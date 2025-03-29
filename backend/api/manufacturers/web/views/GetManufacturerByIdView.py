from database.db import manufacturers
from functions.helpers import get_user_by_token, get_entity_by_id, datetime_to_timestamp


class GetManufacturerByIdView:

    async def __call__(self, token: str, idx: int):
        """Получение производителя по ID"""
        user = await get_user_by_token(token)
        manufacturer_db = await get_entity_by_id(manufacturers, idx, user.cashbox_id)
        manufacturer_db = datetime_to_timestamp(manufacturer_db)
        return manufacturer_db
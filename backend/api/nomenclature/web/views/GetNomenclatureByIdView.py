from api.nomenclature.infrastructure.readers.core.INomenclatureReader import INomenclatureReader
from api.prices.infrastructure.readers.core.IPricesReader import IPricesReader
from functions.helpers import get_user_by_token, datetime_to_timestamp, nomenclature_unit_id_to_name


class GetNomenclatureByIdView:

    def __init__(
        self,
        nomenclature_reader: INomenclatureReader,
        prices_reader: IPricesReader
    ):
        self.__nomenclature_reader = nomenclature_reader
        self.__prices_reader = prices_reader

    async def __call__(
        self,
        token: str, idx: int, with_prices: bool = False
    ):
        user = await get_user_by_token(token)

        nomenclature_db = await self.__nomenclature_reader.get_by_id_with_prices(
            id=idx,
            cashbox_id=user.cashbox_id
        )
        nomenclature_db = datetime_to_timestamp(nomenclature_db)

        if with_prices:
            prices = await self.__prices_reader.get_by_nomenclature_id(
                id=idx,
            )
            nomenclature_db["prices"] = prices

        return nomenclature_db
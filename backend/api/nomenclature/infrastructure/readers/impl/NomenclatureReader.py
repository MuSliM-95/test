from sqlalchemy import select, func

from api.nomenclature.infrastructure.readers.core.INomenclatureReader import INomenclatureReader
from database.db import nomenclature, database, prices, price_types, units


class NomenclatureReader(INomenclatureReader):

    async def get_by_id_with_prices(self, id: int, cashbox_id: int):
        query = (
            select(
                nomenclature,
                units.c.name.label("unit_name")
            )
            .select_from(
                nomenclature
                .outerjoin(units, units.c.id == nomenclature.c.unit)
            )
            .where(
                nomenclature.c.id == id,
                nomenclature.c.cashbox == cashbox_id,
                nomenclature.c.is_deleted.is_not(True)
            )
        )
        nomenclature_info = await database.fetch_one(query)
        return nomenclature_info

from sqlalchemy import and_

from api.nomenclature_groups.infrastructure.functions.core.IDelNomenclatureFromGroupFunction import \
    IDelNomenclatureFromGroupFunction
from database.db import nomenclature_groups_value, database


class DelNomenclatureFromGroupFunction(IDelNomenclatureFromGroupFunction):

    async def __call__(self, group_id: int, nomenclature_id: int):
        query = (
            nomenclature_groups_value.delete()
            .where(and_(
                nomenclature_groups_value.c.group_id == group_id,
                nomenclature_groups_value.c.nomenclature_id == nomenclature_id
            ))
        )
        await database.execute(query)
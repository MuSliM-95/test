from sqlalchemy import and_

from api.nomenclature_groups.infrastructure.functions.core.IPatchNomenclatureGroupFunction import \
    IPatchNomenclatureGroupFunction
from database.db import nomenclature_groups, database


class PatchNomenclatureGroupFunction(IPatchNomenclatureGroupFunction):

    async def __call__(
        self,
        group_id: int,
        name: str
    ):
        query = (
            nomenclature_groups.update()
            .where(and_(
                nomenclature_groups.c.id == group_id,
                nomenclature_groups.c.cashbox == cashbox
            ))
            .values(name=name)
        )
        await database.execute(query)
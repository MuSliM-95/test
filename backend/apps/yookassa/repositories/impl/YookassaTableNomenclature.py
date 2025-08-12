from sqlalchemy import select

from apps.yookassa.repositories.core.IYookassaTableNomenclature import IYookassaTableNomenclature
from database.db import database, nomenclature


class YookassaTableNomenclature(IYookassaTableNomenclature):
    async def fetch_one_by_id(self, nomenclature_id: int):
        return await database.fetch_one(select(nomenclature).where(nomenclature.c.id == nomenclature_id))


from typing import List

from sqlalchemy import and_, select

from api.nomenclature_groups.infrastructure.models.NomenclatureGroupModel import NomenclatureGroupModel
from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import INomenclatureGroupsReader
from database.db import nomenclature_groups, database, nomenclature, nomenclature_groups_value, \
    nomenclature_attributes_value, nomenclature_attributes


class NomenclatureGroupsReader(INomenclatureGroupsReader):

    async def get_nomen_with_attr(self, group_id: int, cashbox_id: int):
        query = (
            select(
                nomenclature.c.id,
                nomenclature_attributes_value.c.value,
                nomenclature_attributes.c.name,
                nomenclature_attributes.c.alias
            )
            .select_from(nomenclature)
            .join(
                nomenclature_groups_value, nomenclature_groups_value.c.nomenclature_id == nomenclature.c.id
            )
            .join(
                nomenclature_attributes_value, nomenclature.c.id == nomenclature_attributes_value.c.nomenclature_id
            )
            .join(
                nomenclature_attributes, nomenclature_attributes.c.id == nomenclature_attributes_value.c.attribute_id
            )
            .where(and_(
                nomenclature_groups_value.c.group_id == group_id,
                nomenclature.c.cashbox == cashbox_id
            ))
        )

        result = await database.fetch_all(query)
        return result

    async def get_group_by_id(
        self,
        id: int,
        cashbox_id: int
    ) -> NomenclatureGroupModel:
        query = (
            nomenclature_groups.select()
            .where(and_(
                nomenclature_groups.c.id == id,
                nomenclature_groups.c.cashbox == cashbox_id
            ))
        )
        result = await database.fetch_one(query)
        return NomenclatureGroupModel(
            id=result.id,
            cashbox_id=result.cashbox,
            name=result.name,
        )

    async def get_all(self, limit: int, offset: int, cashbox_id: int) -> List[NomenclatureGroupModel]:
        query = (
            nomenclature_groups.select()
            .where(
                nomenclature_groups.c.cashbox == cashbox_id
            )
            .limit(limit)
            .offset(offset)
        )
        results = await database.fetch_all(query)
        return [
            NomenclatureGroupModel(
                id=result.id,
                cashbox_id=result.cashbox,
                name=result.name,
            ) for result in results
        ]
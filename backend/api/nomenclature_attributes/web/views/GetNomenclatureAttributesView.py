from fastapi import HTTPException
from sqlalchemy import select, func, and_

from api.nomenclature_attributes.web.models import schemas
from database.db import nomenclature_attributes, nomenclature_attributes_value, nomenclature, database
from functions.helpers import get_user_by_token


class GetNomenclatureAttributesView:

    async def __call__(self, token: str, nomenclature_id: int):
        user = await get_user_by_token(token)

        query = (
            select(
                nomenclature_attributes.c.id,
                nomenclature_attributes.c.name,
                nomenclature_attributes.c.alias,
                func.array_agg(nomenclature_attributes_value.c.value).label("attribute_values"),
            )
            .select_from(
                nomenclature_attributes_value
                .join(
                    nomenclature_attributes,
                    nomenclature_attributes_value.c.attribute_id == nomenclature_attributes.c.id,
                )
                .join(
                    nomenclature,
                    nomenclature_attributes_value.c.nomenclature_id == nomenclature.c.id
                )
            )
            .where(and_(
                nomenclature_attributes_value.c.nomenclature_id == nomenclature_id,
                nomenclature.c.cashbox == user.cashbox_id,
                nomenclature_attributes.c.cashbox == user.cashbox_id
            ))
            .group_by(
                nomenclature_attributes.c.id,
                nomenclature_attributes.c.name,
                nomenclature_attributes.c.alias,
            )
        )

        results = await database.fetch_all(query)

        if not results:
            raise HTTPException(status_code=404,
                                detail=f"Номенклатура с ID {nomenclature_id} не найдена или не имеет атрибутов.")

        return schemas.NomenclatureWithAttributesResponse(
            nomenclature_id=nomenclature_id,
            attributes=[
                schemas.AttributeResponse(
                    id=result.id,
                    name=result.name,
                    alias=result.alias,
                    values=result.attribute_values,
                ) for result in results
            ]
        )
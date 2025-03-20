from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from api.nomenclature_attributes.web.models import schemas
from database.db import nomenclature_attributes, database
from functions.helpers import get_user_by_token


class CreateNomenclatureAttributesView:

    async def __call__(
        self,
        token: str,
        attribute_data: schemas.AttributeCreate,
    ):
        user = await get_user_by_token(token)

        try:
            query = nomenclature_attributes.insert().values(
                name=attribute_data.name,
                alias=attribute_data.alias,
                cashbox=user.cashbox_id,
            )
            new_attribute_id = await database.execute(query)
        except IntegrityError:
            raise HTTPException(status_code=400, detail=f"Атрибут с именем '{attribute_data.name}' уже существует.")

        return schemas.AttributeCreateResponse(id=new_attribute_id, name=attribute_data.name,
                                               alias=attribute_data.alias)

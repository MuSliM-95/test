import api.nomenclature_attributes.schemas as schemas

from typing import List
from sqlalchemy.exc import IntegrityError

from database.db import database, nomenclature, nomenclature_attributes, nomenclature_attributes_value
from fastapi import APIRouter, HTTPException

from functions.helpers import get_user_by_token
from sqlalchemy import select, and_

router = APIRouter(tags=["nomenclature_attributes"])


@router.post("/nomenclature/attributes", response_model=schemas.AttributeCreateResponse)
async def new_nomenclature_attributes(
    token: str,
    attribute_data: schemas.AttributeCreate,
) -> schemas.AttributeCreateResponse:

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

    return schemas.AttributeCreateResponse(id=new_attribute_id, name=attribute_data.name, alias=attribute_data.alias)


@router.post("/nomenclature/attributes_value", response_model=schemas.AttributeValueResponse)
async def new_nomenclature_attribute_value(
    token: str,
    attribute_value_data: schemas.AttributeValueCreate
):
    user = await get_user_by_token(token)

    query = (
        select(nomenclature.c.id)
        .where(and_(
            nomenclature.c.id == attribute_value_data.nomenclature_id,
            nomenclature.c.cashbox == user.cashbox_id,
        ))
    )

    nomenclature_record = await database.fetch_one(query)
    if not nomenclature_record:
        raise HTTPException(
            status_code=404, detail=f"Номенклатура с ID '{attribute_value_data.nomenclature_id}' не найдена"
        )

    attribute_ids = [attribute.attribute_id for attribute in attribute_value_data.attributes]

    query = (
        select(
            nomenclature_attributes.c.id
        )
        .where(and_(
            nomenclature_attributes.c.id.in_(attribute_ids),
            nomenclature_attributes.c.cashbox == user.cashbox_id
        ))
    )
    existing_attribute_ids = {record["id"] for record in await database.fetch_all(query)}

    query = select(nomenclature_attributes_value.c.attribute_id, nomenclature_attributes_value.c.value).where(
        nomenclature_attributes_value.c.nomenclature_id == attribute_value_data.nomenclature_id
    )
    existing_values = await database.fetch_all(query)

    existing_values_map = {}
    for record in existing_values:
        if record["attribute_id"] not in existing_values_map:
            existing_values_map[record["attribute_id"]] = set()
        existing_values_map[record["attribute_id"]].add(record["value"])

    attributes_to_insert = []
    for attribute in attribute_value_data.attributes:
        if attribute.attribute_id not in existing_attribute_ids:
            raise HTTPException(
                status_code=404, detail=f"Атрибут с ID '{attribute.attribute_id}' не найден"
            )
        for single_value in attribute.value:
            if single_value in existing_values_map.get(attribute.attribute_id, set()):
                raise HTTPException(
                    status_code=400,
                    detail=f"Значение '{single_value}' для атрибута с ID '{attribute.attribute_id}' уже существует."
                )
            attributes_to_insert.append({
                "nomenclature_id": attribute_value_data.nomenclature_id,
                "attribute_id": attribute.attribute_id,
                "value": single_value
            })

    try:
        query = nomenclature_attributes_value.insert()
        await database.execute_many(query, attributes_to_insert)
    except IntegrityError:
        raise HTTPException(
            status_code=400,
            detail="Ошибка: Уже существует значение для этого атрибута и номенклатуры."
        )

    return schemas.AttributeValueResponse(
        nomenclature_id=attribute_value_data.nomenclature_id,
        attributes=attribute_value_data.attributes
    )


@router.get("/nomenclature/{nomenclature_id}/attributes", response_model=List[schemas.NomenclatureAttribute])
async def get_nomenclature_attributes(nomenclature_id: int, token: str):
    user = await get_user_by_token(token)

    query = (
        select(
            nomenclature_attributes.c.name.label("attribute_name"),
            nomenclature_attributes_value.c.value.label("attribute_value"),
        )
        .select_from(
            nomenclature_attributes_value
            .join(
                nomenclature_attributes,
                nomenclature_attributes_value.c.attribute_id == nomenclature_attributes.c.id,
            )
        )
        .where(nomenclature_attributes_value.c.nomenclature_id == nomenclature_id)
        .order_by(nomenclature_attributes.c.name)
    )

    results = await database.fetch_all(query)

    if not results:
        raise HTTPException(status_code=404, detail=f"Номенклатура с ID {nomenclature_id} не найдена или не имеет атрибутов.")

    # Преобразуем результаты в ожидаемый формат
    attributes = [
        schemas.NomenclatureAttribute(name=row["attribute_name"], value=row["attribute_value"])
        for row in results
    ]

    return attributes
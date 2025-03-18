from typing import List
from sqlalchemy.exc import IntegrityError
import api.nomenclature_attributes.schemas as schemas
from database.db import categories, database, manufacturers, nomenclature, nomenclature_barcodes, prices, price_types, \
    warehouse_register_movement, warehouses, units, warehouse_balances, nomenclature_attributes, nomenclature_attributes_value, nomenclature_groups, nomenclature_relations
from fastapi import APIRouter, HTTPException

from functions.helpers import (
    check_entity_exists,
    check_unit_exists,
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
    nomenclature_unit_id_to_name,
)
from sqlalchemy import func, select, and_, desc, asc, case, cast, ARRAY, null, or_, insert

router = APIRouter(tags=["nomenclature_attributes"])


@router.post("/nomenclature/attributes", response_model=schemas.AttributeCreateResponse)
async def new_nomenclature_attributes(
    token: str,
    attribute_data: schemas.AttributeCreate,
) -> schemas.AttributeCreateResponse:
    """
    Создает новый атрибут для номенклатуры.

    :param token: Токен для проверки пользователя.
    :param attribute_data: Данные для создания атрибута.
    :return: Созданный атрибут.

    """

    # Проверяем токен пользователя
    user = await get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Недействительный токен")

    # Проверяем, существует ли атрибут с таким же именем
    query = select(nomenclature_attributes).where(nomenclature_attributes.c.name == attribute_data.name)
    existing_attribute = await database.fetch_one(query)

    if existing_attribute:
        raise HTTPException(status_code=400, detail=f"Атрибут с именем '{attribute_data.name}' уже существует.")

    # Создаем новый атрибут
    query = nomenclature_attributes.insert().values(
        name=attribute_data.name,
        alias=attribute_data.alias,
        cash_box=user.cashbox_id,
    )
    new_attribute_id = await database.execute(query)

    return schemas.AttributeCreateResponse(id=new_attribute_id, name=attribute_data.name, alias=attribute_data.alias)


@router.post("/nomenclature/attributes_value", response_model=schemas.AttributeValueResponse)
async def new_nomenclature_attribute_value(
    token: str,
    attribute_value_data: schemas.AttributeValueCreate
):
    """
    Присваивает значения атрибутов номенклатуре.

    :param token: Токен пользователя.
    :param attribute_value_data: Данные для создания значения атрибутов.
    :return: Подтверждение сохранения.

    """

    # Проверка токена пользователя
    user = await get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Недействительный токен")

    # Проверка существования номенклатуры
    query = select(nomenclature.c.id).where(and_(
        nomenclature.c.id == attribute_value_data.nomenclature_id,
        nomenclature.c.cashbox == user.cashbox_id,
    ))

    nomenclature_record = await database.fetch_one(query)
    if not nomenclature_record:
        raise HTTPException(
            status_code=404, detail=f"Номенклатура с ID '{attribute_value_data.nomenclature_id}' не найдена"
        )

    # Проверка существования атрибутов одним запросом
    attribute_ids = [attribute.attribute_id for attribute in attribute_value_data.attributes]
    query = select(nomenclature_attributes.c.id).where(nomenclature_attributes.c.id.in_(attribute_ids))
    existing_attribute_ids = {record["id"] for record in await database.fetch_all(query)}

    # Получение уже существующих значений для номенклатуры и атрибутов
    query = select(nomenclature_attributes_value.c.attribute_id, nomenclature_attributes_value.c.value).where(
        nomenclature_attributes_value.c.nomenclature_id == attribute_value_data.nomenclature_id
    )
    existing_values = await database.fetch_all(query)

    # Сопоставление существующих значений с их атрибутами
    existing_values_map = {}
    for record in existing_values:
        if record["attribute_id"] not in existing_values_map:
            existing_values_map[record["attribute_id"]] = set()
        existing_values_map[record["attribute_id"]].add(record["value"])

    # Формирование данных для вставки
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

    # Вставка данных в таблицу
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
    """
    Возвращает список атрибутов и их значений для заданной номенклатуры.

    :param nomenclature_id: ID номенклатуры.
    :param token: Токен пользователя.

    """

    # Проверка токена пользователя
    user = await get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Недействительный токен")

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


@router.post("/nomenclature/make_groups", response_model=dict)
async def create_nomenclature_group(token: str, data: schemas.NomenclatureRelations):
    """
    Объединяет номенклатуры в группу.

    :param token: Токен пользователя.
    :param data: Список ID номенклатур для объединения в группу.

    """

    # Проверка токена пользователя
    user = await get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Недействительный токен")

    # Проверяем, что список ID не пустой
    if not data.nomenclature_ids or len(data.nomenclature_ids) < 2:
        raise HTTPException(
            status_code=400,
            detail="Для создания группы необходимо указать как минимум два ID номенклатур."
        )

    # Проверка существования всех номенклатур
    existing_nomenclatures = await database.fetch_all(
        select(nomenclature.c.id).where(nomenclature.c.id.in_(data.nomenclature_ids))
    )
    existing_ids = {row["id"] for row in existing_nomenclatures}

    if len(existing_ids) != len(data.nomenclature_ids):
        raise HTTPException(
            status_code=404,
            detail="Один или несколько указанных ID номенклатур не существуют."
    )

    # Создаем новую группу
    new_group_id = await database.execute(
        nomenclature_groups.insert().values()
    )

    # Формируем данные для вставки
    insert_data = [
        {
            "nomenclature_id": nomenclature_id,
            "group_id": new_group_id,
        }
        for nomenclature_id in data.nomenclature_ids
    ]

    # Вставка всех записей в таблицу nomenclature_relations
    try:
        async with database.transaction():
            await database.execute_many(
                nomenclature_relations.insert(),
                insert_data
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось создать группу: {str(e)}")

    return {"message": "Группа успешно создана", "group_id": new_group_id}

@router.get("/nomenclature/{nomenclature_id}/group", response_model=schemas.NomenclatureGroupResponse)
async def get_group_for_nomenclature(token: str, nomenclature_id: int):
    """
    Получает group_id для указанной номенклатуры.

    :param token: Токен пользователя.
    :param nomenclature_id: ID номенклатуры.

    """

    # Проверка токена пользователя
    user = await get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Недействительный токен")

    # Запрос для получения group_id
    query = select(nomenclature_relations.c.group_id).where(
        nomenclature_relations.c.nomenclature_id == nomenclature_id
    )
    result = await database.fetch_one(query)

    if not result or not result["group_id"]:
        raise HTTPException(
            status_code=404,
            detail=f"Группа не найдена для номенклатуры с ID {nomenclature_id}"
        )

    return schemas.NomenclatureGroupResponse(
        nomenclature_id=nomenclature_id,
        group_id=result["group_id"]
    )

@router.post("/nomenclature/{group_id}/add", response_model=schemas.AddNomenclatureResponse)
async def add_nomenclature_to_group(token: str, group_id: int, nomenclature_id: int):
    """
    Добавляет номенклатуру в существующую группу.

    :param token: Токен пользователя.
    :param group_id: ID группы, в которую нужно добавить номенклатуру.
    :param nomenclature_id: ID номенклатуры, которую нужно добавить.

    """

    # Проверка токена пользователя
    user = await get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Недействительный токен")

    # Проверка существования group_id
    group_exists = await database.fetch_one(
        select(nomenclature_groups.c.id).where(nomenclature_groups.c.id == group_id)
    )
    if not group_exists:
        raise HTTPException(status_code=404, detail=f"Группа с ID {group_id} не существует")

    # Проверка существования номенклатуры
    nomenclature_exists = await database.fetch_one(
        select(nomenclature.c.id).where(nomenclature.c.id == nomenclature_id)
    )
    if not nomenclature_exists:
        raise HTTPException(
            status_code=404, detail=f"Номенклатура с ID {nomenclature_id} не существует"
        )

    # Проверка, что номенклатура ещё не в группе
    already_in_group = await database.fetch_one(
        select(nomenclature_relations.c.id).where(
            (nomenclature_relations.c.group_id == group_id) &
            (nomenclature_relations.c.nomenclature_id == nomenclature_id)
        )
    )
    if already_in_group:
        raise HTTPException(
            status_code=400,
            detail=f"Номенклатура с ID {nomenclature_id} уже находится в группе с ID {group_id}"
        )

    # Добавляем номенклатуру в группу
    try:
        query = insert(nomenclature_relations).values(
            nomenclature_id=nomenclature_id,
            group_id=group_id
        )
        await database.execute(query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось добавить номенклатуру: {str(e)}")

    return schemas.AddNomenclatureResponse(
        message="Номенклатура успешно добавлена в группу",
        group_id=group_id,
        nomenclature_id=nomenclature_id
    )

@router.get("/nomenclature/groups/{group_id}", response_model=List[int])
async def get_nomenclatures_by_group(token: str, group_id: int):
    """
    Возвращает список ID всех номенклатур из указанной группы.

    :param token: Токен пользователя.
    :param group_id: ID группы.

    """

    # Проверка токена пользователя
    user = await get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Недействительный токен")

    # Проверка существования группы
    group_exists = await database.fetch_one(
        select(nomenclature_groups.c.id).where(nomenclature_groups.c.id == group_id)
    )
    if not group_exists:
        raise HTTPException(status_code=404, detail=f"Группа с ID {group_id} не существует")

    # Запрос на получение ID номенклатур, связанных с указанной группой
    query = select(nomenclature_relations.c.nomenclature_id).where(
        nomenclature_relations.c.group_id == group_id
    )

    results = await database.fetch_all(query)

    if not results:
        raise HTTPException(status_code=404, detail=f"Номенклатуры не найдены для группы с ID {group_id}")

    # Преобразуем результаты в список ID номенклатур
    nomenclature_ids = [row["nomenclature_id"] for row in results]

    return nomenclature_ids
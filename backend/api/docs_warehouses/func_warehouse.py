import databases.core
from fastapi import HTTPException
from database.db import database
from database.db import (
    warehouse_register_movement,
    docs_warehouse,
    docs_warehouse_goods,
    OperationType,
    users_cboxes_relation,
    organizations,
    contracts,
    docs_purchases,
    docs_sales,
    warehouses,
    contragents,
    nomenclature,
    pictures)
from sqlalchemy.sql import select, func, case, and_
from functions.helpers import get_user_by_token
from api.docs_warehouses.schemas import WarehouseOperations


async def check_relationship(entity):
    exeptions = set()
    try:
        if not await database.fetch_one(docs_purchases.select().where(
                docs_purchases.c.id == entity['docs_purchases'])):
            exeptions.add(f"error not found docs_purchases.id = {entity['docs_purchases']}")
            del entity['docs_purchases']

        if not await database.fetch_one(docs_sales.select().where(
                docs_sales.c.id == entity['docs_sales_id'])):
            exeptions.add(f"error not found docs_sales.id = {entity['docs_sales_id']}")
            del entity['docs_sales_id']

        if not await database.fetch_one(warehouses.select().where(
                warehouses.c.id == entity['warehouse'])):
            exeptions.add(f"error not found warehouse in warehouses.id = {entity['warehouse']}")
            del entity['warehouse']

        if not await database.fetch_one(organizations.select().where(
                organizations.c.id == entity['organization'])):
            exeptions.add(f"error not found warehouse in organizations.id = {entity['organization']}")
            del entity['organization']

        if not await database.fetch_one(warehouses.select().where(
                warehouses.c.id == entity['to_warehouse'])):
            exeptions.add(f"error not found to_warehouse in warehouses.id = {entity['to_warehouse']}")
            del entity['to_warehouse']

        if not await database.fetch_one(contragents.select().where(
                contragents.c.id == entity['contragent'])):
            del entity['contragent']

        return {"error": exeptions, "entity": entity}
    except Exception as error:
        raise Exception(f"error check doc_warehouse failed {error}")


async def set_data_doc_warehouse(**kwargs):
    await get_user_by_token(kwargs.get('token'))

    users_cboxes = await database.fetch_one(
        users_cboxes_relation.select().where(users_cboxes_relation.c.token == kwargs.get('token'))
    )
    # check = await check_relationship(kwargs.get('entity_values'))

    entity = kwargs.get('entity_values')

    if entity['status'] is None:
        entity['status'] = False

    entity['created_by'] = users_cboxes.get('user')
    entity['cashbox'] = users_cboxes.get('cashbox_id')
    entity['is_deleted'] = False

    return entity


async def insert_docs_warehouse(entity):
    try:
        del entity["goods"]
        query = docs_warehouse.insert().values(entity)
        doc_id = await database.execute(query)
    except Exception as err:
        raise Exception(f"error insert record in docs_warehouse: {str(err)}")
    return doc_id


@database.transaction()
async def update_docs_warehouse(entity):
    try:
        if entity.get('goods'):
            del entity["goods"]
        query = docs_warehouse.update()\
            .where(docs_warehouse.c.id == entity['id'])\
            .values(entity)
        await database.execute(query)
        query = await database.fetch_one(docs_warehouse.select().where(docs_warehouse.c.id == entity['id']))
        print(query)
    except Exception as err:
        raise Exception(f"error update record in docs_warehouse: {str(err)}")
    return query['id']


@database.transaction()
async def update_goods_warehouse(entity, doc_id, type_operation):
    try:
    
        items_sum = 0

        goods_db = [dict(item) for item in await database.fetch_all(
            docs_warehouse_goods
            .select()
            .where(docs_warehouse_goods.c.docs_warehouse_id == doc_id))]
        ids_good = [good['id'] for good in goods_db]
        query = docs_warehouse_goods.delete().where(docs_warehouse_goods.c.id.in_(ids_good))
        await database.execute(query)
        if not entity['status']:
            try:
                query = warehouse_register_movement.delete().where(
                    and_(warehouse_register_movement.c.document_warehouse_id == doc_id,
                         warehouse_register_movement.c.type_amount == type_operation
                         ))
                await database.execute(query)
                for item in entity.get('goods'):
                    item = dict(item)
                    item['docs_warehouse_id'] = doc_id
                    if not item.get("unit"):
                        q = nomenclature.select().where(nomenclature.c.id == item['nomenclature'])
                        nom_db = await database.fetch_one(q)
                        item['unit'] = nom_db.unit
                    query = docs_warehouse_goods.insert().values(item)
                    await database.execute(query)
            except Exception as err:
                raise Exception(f"error delete record in warehouse_register_movement: {str(err)}")
        else:
            query = warehouse_register_movement.delete().where(
                and_(
                    warehouse_register_movement.c.document_warehouse_id == doc_id,
                    warehouse_register_movement.c.type_amount == type_operation
                    ))
            await database.execute(query)
            for item in entity.get('goods'):
                item = dict(item)
                item['docs_warehouse_id'] = doc_id
                if not item.get("unit"):
                    q = nomenclature.select().where(nomenclature.c.id == item['nomenclature'])
                    nom_db = await database.fetch_one(q)
                    item['unit'] = nom_db.unit
                query = docs_warehouse_goods.insert().values(item)
                await database.execute(query)
                try:
                    query = warehouse_register_movement.insert().values(
                                {
                                    "organization_id": entity["organization"],
                                    "type_amount": type_operation,
                                    "warehouse_id": entity["warehouse"],
                                    "document_sale_id": entity.get('docs_sales_id'),
                                    "document_purchase_id": entity.get('docs_purchases'),
                                    "nomenclature_id": item["nomenclature"],
                                    "document_warehouse_id": doc_id,
                                    "amount": item['quantity'],
                                    "cashbox_id": entity['cashbox'],
                                })
                    await database.execute(query)
                except Exception as err:
                    raise Exception(f"error update record in warehouse_register_movement: {str(err)}")
        query = (
            docs_warehouse.update()
            .where(docs_warehouse.c.id == doc_id)
            .values({"sum": items_sum})
        )
        await database.execute(query)
    except Exception as err:
        raise HTTPException(f"error {err}")


async def check_exist_amount(goods, warehouse):
    for good in goods:
        q = case(
            [
                (
                    warehouse_register_movement.c.type_amount == 'minus',
                    warehouse_register_movement.c.amount * (-1)
                )
            ],
            else_=warehouse_register_movement.c.amount

        )
        query = (
            select(
                nomenclature.c.name,
                func.sum(q).label("current_amount"))
            .where(
                warehouse_register_movement.c.warehouse_id == warehouse,
                warehouse_register_movement.c.nomenclature_id == good["nomenclature"]
            )
        ).group_by(warehouse_register_movement.c.nomenclature_id, nomenclature.c.name) \
            .select_from(warehouse_register_movement
                         .join(nomenclature,
                               warehouse_register_movement.c.nomenclature_id == nomenclature.c.id
                               ))
        good_db = await database.fetch_one(query)
        if good_db['current_amount'] >= good["quantity"]:
            continue
        else:
            raise Exception(f"there is not enough balance to outgoing good = {good}")


async def insert_goods(entity, doc_id, type_operation, not_create_goods: bool = False):
    try:
        items_sum = 0
        for item in entity.get('goods'):
            if not not_create_goods:
                item["docs_warehouse_id"] = doc_id
                if not item.get("unit"):
                    q = nomenclature.select().where(nomenclature.c.id == item['nomenclature'])
                    nom_db = await database.fetch_one(q)
                    item['unit'] = nom_db.unit
                query = docs_warehouse_goods.insert().values(item)
                await database.execute(query)
                items_sum += item["price"] * item["quantity"]
            try:
                if entity['status']:
                    query = warehouse_register_movement.insert().values(
                        {
                            "organization_id": entity["organization"],
                            "type_amount": type_operation,
                            "warehouse_id": entity["warehouse"],
                            "nomenclature_id": item["nomenclature"],
                            "document_warehouse_id": doc_id,
                            "amount": item['quantity'],
                            "cashbox_id": entity['cashbox'],
                        })
                    await database.execute(query)
            except Exception as err:
                raise Exception(f"error insert record warehouse_register_movement: {str(err)}")
        query = (
            docs_warehouse.update()
            .where(docs_warehouse.c.id == doc_id)
            .values({"sum": items_sum})
        )
        await database.execute(query)
    except Exception as error:
        raise Exception(f"error: {str(error)}")

@database.transaction()
async def incoming(entity_values, token) -> int:
    """
    подготавливаем словарь для doc_warehouse
    создаем связи doc_warehouse <-> goods
    создаем записи движения goods в warehouse_register_movement

    return Id созданного документа doc_warehouse
    """
    try:
        goods: list = entity_values['goods']
        entity = await set_data_doc_warehouse(entity_values=entity_values, token=token)
        doc_id = await insert_docs_warehouse(entity=entity)
        entity.update({'goods': goods})
        await insert_goods(entity=entity, doc_id=doc_id, type_operation=OperationType.plus)
        return doc_id
    except Exception as error:
        raise HTTPException(status_code=433, detail=str(error))


async def outgoing(entity_values, token):
    """
    подготавливаем словарь для doc_warehouse
    создаем связи doc_warehouse <-> goods
    проверяем досутпность списания - остаток >= goods.quantity
    создаем записи движения goods в warehouse_register_movement

    return Id созданного документа doc_warehouse
    """
    try:
        goods: list = entity_values['goods']
        # await check_exist_amount(goods=goods, warehouse=entity_values["warehouse"])
        entity = await set_data_doc_warehouse(entity_values=entity_values, token=token)
        doc_id = await insert_docs_warehouse(entity=entity)
        entity.update({'goods': goods})
        await insert_goods(entity=entity, doc_id=doc_id, type_operation=OperationType.minus)
        return doc_id
    except Exception as error:
        raise HTTPException(status_code=433, detail=str(error))

@database.transaction()
async def write_off(entity_values, token):
    """
    Списание товара — частный случай расхода, когда нужно обязательное фото (зависит от настроек)
    и уменьшение остатка (OperationType.minus).
    """
    try:
        goods: list = entity_values['goods']
        entity = await set_data_doc_warehouse(entity_values=entity_values, token=token)
        doc_id = await insert_docs_warehouse(entity=entity)
        entity.update({'goods': goods})
        await insert_goods(entity=entity, doc_id=doc_id, type_operation=OperationType.minus)
        return doc_id
    except Exception as error:
        raise HTTPException(status_code=433, detail=str(error))

@database.transaction()
async def transfer(entity_values, token):
    """
    подготавливаем словарь для doc_warehouse
    создаем связи doc_warehouse <-> goods
    создаем записи движения выполняя outgoing и incoming

    return Id созданного документа doc_warehouse
    """
    try:
        goods: list = entity_values['goods']
        entity = await set_data_doc_warehouse(entity_values=entity_values, token=token)
        # await check_exist_amount(goods=goods, warehouse=entity_values["warehouse"])
        doc_id = await insert_docs_warehouse(entity=entity)

        entity.update({'goods': goods})
        await insert_goods(entity=entity, doc_id=doc_id, type_operation=OperationType.minus)
        entity.update({'warehouse': entity['to_warehouse']})
        await insert_goods(entity=entity, doc_id=doc_id, type_operation=OperationType.plus, not_create_goods=True)
        return doc_id
    except Exception as error:
        raise HTTPException(status_code=433, detail=str(error))


async def call_type_movement(t, **kwargs):
    getMethod = {
        'incoming': incoming,
        'outgoing': outgoing,
        'transfer': transfer,
        'write_off': write_off
    }
    if t in getMethod:
        return await getMethod[t](**kwargs)
    else:
        raise HTTPException(status_code=422, detail=f"error method [{t}] does not exist")


async def validate_photo_for_writeoff(entity_id: int):
    """
    Проверка наличия фото для списания товара
    """
    doc_query = docs_warehouse.select().where(
        docs_warehouse.c.id == entity_id,
        docs_warehouse.c.operation == "write_off",
        docs_warehouse.c.is_deleted.is_not(True),
    )

    doc = await database.fetch_one(doc_query)

    if not doc:
        return

    picture_query = pictures.select().where(
        pictures.c.entity_id == entity_id,
        pictures.c.entity == 'docs_warehouse',
        pictures.c.is_deleted.is_not(True),
    )

    picture = await database.fetch_one(picture_query)

    if not picture:
            raise HTTPException(
            status_code=422,
            detail="Для проведения документа со списанием необходимо прикрепить хотя бы одну фотографию."
        )

    return picture
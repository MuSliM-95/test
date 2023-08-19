from fastapi import APIRouter, HTTPException

from database.db import database, contracts, organizations

import api.contracts.schemas as schemas

from functions.helpers import datetime_to_timestamp, get_entity_by_id, check_contragent_exists, \
    check_entity_exists
from functions.helpers import get_user_by_token

from ws_manager import manager
from typing import Optional

router = APIRouter(tags=["contracts"])


@router.get("/contracts/{idx}", response_model=schemas.Contract)
async def get_contract_by_id(token: str, idx: int):
    """Получение контракта по ID"""
    user = await get_user_by_token(token)
    contract_db = await get_entity_by_id(contracts, idx, user.id)
    contract_db = datetime_to_timestamp(contract_db)
    return contract_db


@router.get("/contracts/", response_model=schemas.ContractList)
async def get_contracts(token: str, name: Optional[str] = None, limit: int = 100, offset: int = 0):
    """Получение списка контрактов"""
    user = await get_user_by_token(token)
    query = (
        contracts.select()
        .where(
            contracts.c.owner == user.id,
            contracts.c.is_deleted.is_not(True),
        )
        .limit(limit)
        .offset(offset)
    )
    if name:
        query = (
            contracts.select()
            .where(
                contracts.c.owner == user.id,
                contracts.c.is_deleted.is_not(True),
                contracts.c.name.ilike(f"%{name}%")
            )
            .limit(limit)
            .offset(offset)
        )

    contracts_db = await database.fetch_all(query)
    contracts_db = [*map(datetime_to_timestamp, contracts_db)]

    return contracts_db


@router.post("/contracts/", response_model=schemas.ContractList)
async def new_contract(token: str, contracts_data: schemas.ContractCreateMass):
    """Создание контракта"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    contragents_cache = set()
    organizations_cache = set()
    exceptions = []
    for contract_values in contracts_data.dict()["__root__"]:
        contract_values["owner"] = user.id

        if contract_values.get("contragent") is not None:
            if contract_values["contragent"] not in contragents_cache:
                try:
                    await check_contragent_exists(contract_values["contragent"], user.cashbox_id)
                    contragents_cache.add(contract_values["contragent"])
                except HTTPException as e:
                    exceptions.append(str(contract_values) + " " + e.detail)
                    continue

        if contract_values.get("organization") is not None:
            if contract_values["organization"] not in organizations_cache:
                try:
                    await check_entity_exists(organizations, contract_values["organization"], user.id)
                    organizations_cache.add(contract_values["organization"])
                except HTTPException as e:
                    exceptions.append(str(contract_values) + " " + e.detail)
                    continue

        query = contracts.insert().values(contract_values)
        contract_id = await database.execute(query)
        inserted_ids.add(contract_id)

    query = (
        contracts.select()
        .where(
            contracts.c.owner == user.id,
            contracts.c.id.in_(inserted_ids)
        )
    )
    contracts_db = await database.fetch_all(query)
    contracts_db = [*map(datetime_to_timestamp, contracts_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "contracts",
            "result": contracts_db,
        },
    )

    if exceptions:
        raise HTTPException(400, "Не были добавлены следующие записи: " + ", ".join(exceptions))

    return contracts_db


@router.patch("/contracts/{idx}", response_model=schemas.Contract)
async def edit_contract(
    token: str,
    idx: int,
    contract: schemas.ContractEdit,
):
    """Редактирование контракта"""
    user = await get_user_by_token(token)
    contract_db = await get_entity_by_id(contracts, idx, user.id)
    contract_values = contract.dict(exclude_unset=True)

    if contract_values:
        if contract_values.get("contragent") is not None:
            await check_contragent_exists(contract_values["contragent"], user.cashbox_id)

        if contract_values.get("organization") is not None:
            await check_entity_exists(organizations, contract_values["organization"], user.id)

        query = (
            contracts.update()
            .where(contracts.c.id == idx, contracts.c.owner == user.id)
            .values(contract_values)
        )
        await database.execute(query)
        contract_db = await get_entity_by_id(contracts, idx, user.id)

    contract_db = datetime_to_timestamp(contract_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "contracts", "result": contract_db},
    )

    return contract_db


@router.delete("/contracts/{idx}", response_model=schemas.Contract)
async def delete_contract(token: str, idx: int):
    """Удаление контракта"""
    user = await get_user_by_token(token)

    await get_entity_by_id(contracts, idx, user.id)

    query = (
        contracts.update()
        .where(contracts.c.id == idx, contracts.c.owner == user.id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = contracts.select().where(
        contracts.c.id == idx, contracts.c.owner == user.id
    )
    contract_db = await database.fetch_one(query)
    contract_db = datetime_to_timestamp(contract_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "contracts",
            "result": contract_db,
        },
    )

    return contract_db

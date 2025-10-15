from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from api.tech_cards.schemas import TechCardStatus
from api.tech_cards.models import TechCardDB
from api.tech_operations.schemas import (
    TechOperation,
    TechOperationCreate,
    TechOperationComponentCreate,
)
from api.tech_operations.models import (
    TechOperationDB,
    TechOperationComponentDB,
    TechOperationPaymentDB,
)

from database.db import warehouses, warehouse_balances, warehouse_register_movement

from api.deps import get_db, get_user_by_token
import uuid

router = APIRouter(prefix="/tech_operations", tags=["tech_operations"])


@router.post(
    "/",
    response_model=TechOperation,
    status_code=status.HTTP_201_CREATED,
)
async def create_tech_operation(
    token: str, tech_operation: TechOperationCreate, db: Session = Depends(get_db)
):
    user = await get_user_by_token(token, db)
    db_tech_card = db.query(TechCardDB).get(tech_operation.tech_card_id)
    if not db_tech_card:
        raise HTTPException(status_code=404, detail="Tech card not found")

    db_tech_operation = TechOperationDB(
        id=uuid.uuid4(),
        user_id=user.id,
        **tech_operation.dict(exclude={"component_quantities", "payment_ids"}),
        production_order_id=uuid.uuid4(),
        consumption_order_id=uuid.uuid4(),
        status="active",
    )

    # Добавляем компоненты
    for i in tech_operation.component_quantities:
        comp = TechOperationComponentDB(
            id=uuid.uuid4(),
            name=i.name,
            quantity=i.quantity,
            operation_id=db_tech_operation.id,
        )
        db.add(comp)

    # Добавляем платежи
    if tech_operation.payment_ids:
        for payment_id in tech_operation.payment_ids:
            payment = TechOperationPaymentDB(
                id=uuid.uuid4(),
                operation_id=db_tech_operation.id,
                payment_id=payment_id,
            )
            db.add(payment)

    db.add(db_tech_operation)
    db.commit()
    db.refresh(db_tech_operation)

    result = {
        "id": db_tech_operation.id,
        "user_id": db_tech_operation.user_id,
        "tech_card_id": db_tech_operation.tech_card_id,
        "output_quantity": db_tech_operation.output_quantity,
        "from_warehouse_id": db_tech_operation.from_warehouse_id,
        "to_warehouse_id": db_tech_operation.to_warehouse_id,
        "nomenclature_id": db_tech_operation.nomenclature_id,
        "status": db_tech_operation.status,
        "created_at": db_tech_operation.created_at,
        "production_order_id": db_tech_operation.production_order_id,
        "consumption_order_id": db_tech_operation.consumption_order_id,
        "component_quantities": tech_operation.component_quantities,
        "payment_ids": tech_operation.payment_ids,
    }

    return result


@router.post(
    "/bulk",
    response_model=List[TechOperation],
    status_code=status.HTTP_201_CREATED,
)
async def bulk_create_tech_operations(
    token: str, tech_operation: List[TechOperationCreate], db: Session = Depends(get_db)
):
    await get_user_by_token(token, db)
    created_ops = []
    for op in tech_operation:
        created_ops.append(create_tech_operation(token, op, db))
    return created_ops


@router.get("/", response_model=List[TechOperation])
async def get_tech_operations(
    token: str,
    tech_card_id: Optional[uuid.UUID] = None,
    status: Optional[TechCardStatus] = None,
    limit: int = Query(10, ge=1),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    user = await get_user_by_token(token, db)

    query = db.query(TechOperationDB).filter(
        TechOperationDB.status != "deleted",
        TechOperationDB.user_id == user.id,
    )

    if tech_card_id:
        query = query.filter(TechOperationDB.tech_card_id == tech_card_id)
    if status:
        query = query.filter(TechOperationDB.status == status)

    return query.offset(offset).limit(limit).all()


@router.post("/{idx}/cancel", response_model=TechOperation)
async def cancel_tech_operation(
    token: str, idx: uuid.UUID, db: Session = Depends(get_db)
):
    await get_user_by_token(token, db)
    operation = db.query(TechOperationDB).get(idx)
    if not operation:
        raise HTTPException(status_code=404, detail="Operation not found")

    if operation.status != "active":
        raise HTTPException(status_code=400, detail="Operation not active")

    operation.status = "canceled"
    db.commit()
    db.refresh(operation)
    return operation


@router.delete("/{idx}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_tech_operation(
    token: str, idx: uuid.UUID, db: Session = Depends(get_db)
):
    await get_user_by_token(token, db)
    operation = db.query(TechOperationDB).get(idx)
    if not operation:
        raise HTTPException(status_code=404, detail="Operation not found")

    operation.status = "deleted"
    db.commit()


@router.post("/{idx}/items", status_code=status.HTTP_201_CREATED)
async def add_items_to_operation(
    token: str,
    idx: uuid.UUID,
    items: List[TechOperationComponentCreate],
    db: Session = Depends(get_db),
):
    await get_user_by_token(token, db)
    operation = db.query(TechOperationDB).get(idx)
    if not operation:
        raise HTTPException(status_code=404, detail="Operation not found")

    for component_id, quantity in items.items():
        comp = TechOperationComponentDB(
            id=uuid.uuid4(),
            operation_id=idx,
            component_id=component_id,
            quantity=quantity,
        )
        db.add(comp)

    db.commit()
    return {"message": "Items added to operation"}

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
from api.deps import get_db
import uuid

router = APIRouter(prefix="/tech_operations", tags=["tech_operations"])


@router.post(
    "/",
    response_model=TechOperation,
    status_code=status.HTTP_201_CREATED,
)
async def create_tech_operation(
    operation: TechOperationCreate, db: Session = Depends(get_db)
):
    db_tech_card = db.query(TechCardDB).get(operation.tech_card_id)
    if not db_tech_card:
        raise HTTPException(status_code=404, detail="Tech card not found")

    db_operation = TechOperationDB(
        id=uuid.uuid4(),
        **operation.dict(exclude={"component_quantities", "payment_ids"}),
        production_order_id=uuid.uuid4(),
        consumption_order_id=uuid.uuid4(),
        status="active",
    )

    # Добавляем компоненты
    for component_id, quantity in operation.component_quantities.items():
        comp = TechOperationComponentDB(
            id=uuid.uuid4(),
            operation_id=db_operation.id,
            component_id=component_id,
            quantity=quantity,
        )
        db.add(comp)

    # Добавляем платежи
    if operation.payment_ids:
        for payment_id in operation.payment_ids:
            payment = TechOperationPaymentDB(
                id=uuid.uuid4(), operation_id=db_operation.id, payment_id=payment_id
            )
            db.add(payment)

    db.add(db_operation)
    db.commit()
    db.refresh(db_operation)
    return db_operation


@router.post(
    "/bulk",
    response_model=List[TechOperation],
    status_code=status.HTTP_201_CREATED,
)
async def bulk_create_tech_operations(
    operations: List[TechOperationCreate], db: Session = Depends(get_db)
):
    created_ops = []
    for op in operations:
        created_ops.append(create_tech_operation(op, db))
    return created_ops


@router.get("/", response_model=List[TechOperation])
async def get_tech_operations(
    user_id: Optional[int] = None,
    tech_card_id: Optional[uuid.UUID] = None,
    status: Optional[TechCardStatus] = None,
    limit: int = Query(10, ge=1),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    query = db.query(TechOperationDB)

    if user_id:
        query = query.filter(TechOperationDB.user_id == user_id)
    if tech_card_id:
        query = query.filter(TechOperationDB.tech_card_id == tech_card_id)
    if status:
        query = query.filter(TechOperationDB.status == status)

    return query.offset(offset).limit(limit).all()


@router.post("/{idx}/cancel", response_model=TechOperation)
async def cancel_tech_operation(idx: uuid.UUID, db: Session = Depends(get_db)):
    operation = db.query(TechOperationDB).get(idx)
    if not operation:
        raise HTTPException(status_code=404, detail="Operation not found")

    if operation.status != "active":
        raise HTTPException(status_code=400, detail="Operation not active")

    # В реальной системе здесь была бы логика отмены ордеров
    operation.status = "canceled"
    db.commit()
    db.refresh(operation)
    return operation


@router.delete("/{idx}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_tech_operation(idx: uuid.UUID, db: Session = Depends(get_db)):
    operation = db.query(TechOperationDB).get(idx)
    if not operation:
        raise HTTPException(status_code=404, detail="Operation not found")

    # В реальной системе - удаление связанных документов
    operation.status = "deleted"
    db.commit()


@router.post("/{idx}/items", status_code=status.HTTP_201_CREATED)
async def add_items_to_operation(
    idx: uuid.UUID,
    items: List[TechOperationComponentCreate],
    db: Session = Depends(get_db),
):
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

from typing import List, Optional
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException, status, Query
from .models import TechCardDB, TechCardItemDB
from ..deps import get_db, get_user_by_token
from . import schemas
import uuid

router = APIRouter(prefix="/tech_cards", tags=["tech_cards"])


@router.post(
    "/", response_model=schemas.TechCardResponse, status_code=status.HTTP_201_CREATED
)
async def create_tech_card(
    token: str, tech_card: schemas.TechCardCreate, db: Session = Depends(get_db)
):
    user = await get_user_by_token(token, db)
    tech_card_data = tech_card.dict()
    items_data = tech_card_data.pop("items", [])
    db_tech_card = TechCardDB(**tech_card_data, id=uuid.uuid4(), user_id=user.id)
    db.add(db_tech_card)

    for items in items_data:
        db_component = TechCardItemDB(tech_card_id=db_tech_card.id, **items)
        db.add(db_component)

    db.commit()
    db.refresh(db_tech_card)
    return db_tech_card


@router.get("/", response_model=List[schemas.TechCard])
async def get_tech_cards(
    token: str,
    card_type: Optional[schemas.TechCardType] = None,
    # name_item: Optional[str] = None,
    # parent_item_id: Optional[uuid.UUID] = None,
    limit: int = Query(10, ge=1),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    user = await get_user_by_token(token, db)
    query = db.query(TechCardDB).filter(
        TechCardDB.status != "deleted",
        TechCardDB.user_id == user.id,
    )

    if card_type:
        query = query.filter(TechCardDB.card_type == card_type)

    # if name_item:
    #     query = query.join(TechCardDB.items).filter(TechCardItemDB.name == name_item)

    return query.offset(offset).limit(limit).all()


@router.get("/{idx}", response_model=schemas.TechCard)
async def get_tech_card(token: str, idx: uuid.UUID, db: Session = Depends(get_db)):
    await get_user_by_token(token, db)
    db_tech_card = (
        db.query(TechCardDB)
        .filter(TechCardDB.id == idx, TechCardDB.status != "deleted")
        .first()
    )
    if not db_tech_card:
        raise HTTPException(status_code=404, detail="Tech card not found")
    return db_tech_card


@router.put("/{idx}", response_model=schemas.TechCard)
async def update_tech_card(
    token: str,
    idx: uuid.UUID,
    card_data: schemas.TechCardCreate,
    db: Session = Depends(get_db),
):
    await get_user_by_token(token, db)
    db_tech_card = db.query(TechCardDB).get(idx)
    if not db_tech_card:
        raise HTTPException(status_code=404, detail="Tech card not found")

    for field, value in card_data.dict().items():
        setattr(db_tech_card, field, value)

    # db_tech_card.updated_at = func.now()
    db.commit()
    db.refresh(db_tech_card)
    return db_tech_card


@router.delete("/{idx}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_tech_card(token: str, idx: uuid.UUID, db: Session = Depends(get_db)):
    await get_user_by_token(token, db)
    db_tech_card = db.query(TechCardDB).get(idx)
    if not db_tech_card:
        raise HTTPException(status_code=404, detail="Tech card not found")

    db_tech_card.status = "deleted"
    db.commit()


# Методы для работы с компонентами тех. карт
@router.post(
    "/{idx}/items",
    response_model=schemas.TechCardItem,
    status_code=status.HTTP_201_CREATED,
)
async def add_item_to_tech_card(
    token: str,
    idx: uuid.UUID,
    item: schemas.TechCardItemCreate,
    db: Session = Depends(get_db),
):
    await get_user_by_token(token, db)
    db_tech_card = db.query(TechCardDB).get(idx)
    if not db_tech_card:
        raise HTTPException(status_code=404, detail="Tech card not found")

    db_item = TechCardItemDB(id=uuid.uuid4(), tech_card_id=idx, **item.dict())
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item


@router.get("/{idx}/items", response_model=List[schemas.TechCardItem])
async def get_tech_card_items(
    token: str, idx: uuid.UUID, db: Session = Depends(get_db)
):
    await get_user_by_token(token, db)
    db_tech_card = db.query(TechCardDB).get(idx)
    if not db_tech_card:
        raise HTTPException(status_code=404, detail="Tech card not found")
    return db_tech_card.items

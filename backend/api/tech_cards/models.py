from sqlalchemy import (
    Boolean,
    Enum,
    ForeignKey,
    Integer,
    String,
    Column,
    DateTime,
    Float,
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database.db import Base
from sqlalchemy.dialects.postgresql import UUID
import uuid


class TechCardDB(Base):
    __tablename__ = "tech_cards"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(String(1000))
    card_type = Column(Enum("reference", "automatic", name="card_type"), nullable=False)
    auto_produce = Column(Boolean, default=False)  # автосоздания операций
    # Технолог (user_id)
    user_id = Column(Integer, ForeignKey("relation_tg_cashboxes.id"))
    status = Column(
        Enum("active", "canceled", "deleted", name="status"), default="active"
    )
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    items = relationship("TechCardItemDB", back_populates="tech_card")
    operations = relationship("TechOperationDB", back_populates="tech_card")


class TechCardItemDB(Base):
    __tablename__ = "tech_card_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tech_card_id = Column(UUID(as_uuid=True), ForeignKey("tech_cards.id"))
    # component_id = Column(UUID(as_uuid=True), nullable=False)
    name = Column(String(255), nullable=False)
    quantity = Column(Float, nullable=False)
    gross_weight = Column(Float, nullable=True)  # Вес брутто (г, кг)
    net_weight = Column(Float, nullable=True)  # Вес нетто (г, кг)

    tech_card = relationship("TechCardDB", back_populates="items")

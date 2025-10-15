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
    card_type = Column(
        Enum(
            "reference",
            "automatic",
            name="card_type",
            create_type=False,
        ),
        nullable=False,
    )
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
    # name = Column(String(255), nullable=False)
    nomenclature_id = Column(Integer, ForeignKey("nomenclature.id"))
    type_of_processing = Column(String(255), nullable=False)
    waste_from_cold_processing = Column(
        Float, nullable=False
    )  # Отходы от холодной обработки
    waste_from_heat_processing = Column(
        Float, nullable=False
    )  # Отходы при тепловой обработке
    net_weight = Column(Float, nullable=False)  # Вес нетто (г, кг)
    quantity = Column(Float, nullable=False)
    gross_weight = Column(Float, nullable=False)  # Вес брутто (г, кг)
    output = Column(Float, nullable=False)  # Выход

    tech_card = relationship("TechCardDB", back_populates="items")

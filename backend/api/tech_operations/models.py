from sqlalchemy import Column, Integer, Enum, ForeignKey, DateTime, Float, String
from sqlalchemy.sql import func

from sqlalchemy.orm import relationship
from database.db import Base
from sqlalchemy.dialects.postgresql import UUID
import uuid


class TechOperationDB(Base):
    __tablename__ = "tech_operations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tech_card_id = Column(UUID(as_uuid=True), ForeignKey("tech_cards.id"))
    output_quantity = Column(Float, nullable=False)
    from_warehouse_id = Column(Integer, nullable=False)
    to_warehouse_id = Column(Integer, nullable=False)
    # Технолог (user_id)
    user_id = Column(Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), default=func.now())
    nomenclature_id = Column(Integer, ForeignKey("nomenclature.id"))
    status = Column(
        Enum(
            "active",
            "canceled",
            "deleted",
            name="status",
            create_type=False,
        ),
        default="active",
    )
    production_order_id = Column(UUID(as_uuid=True))
    consumption_order_id = Column(UUID(as_uuid=True))

    tech_card = relationship("TechCardDB", back_populates="operations")
    components = relationship("TechOperationComponentDB", back_populates="operation")
    payments = relationship("TechOperationPaymentDB", back_populates="operation")


class TechOperationComponentDB(Base):
    __tablename__ = "tech_operation_components"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    operation_id = Column(UUID(as_uuid=True), ForeignKey("tech_operations.id"))
    # component_id = Column(UUID(as_uuid=True), nullable=False)
    nomeclature_id = Column(Integer, ForeignKey("nomenclature.id"))
    name = Column(String(255), nullable=False)
    quantity = Column(Float, nullable=False)
    gross_weight = Column(Float, nullable=True)  # Вес брутто (г, кг)
    net_weight = Column(Float, nullable=True)  # Вес нетто (г, кг)

    operation = relationship("TechOperationDB", back_populates="components")


class TechOperationPaymentDB(Base):
    __tablename__ = "tech_operation_payments"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    operation_id = Column(UUID(as_uuid=True), ForeignKey("tech_operations.id"))
    payment_id = Column(UUID(as_uuid=True), nullable=False)

    operation = relationship("TechOperationDB", back_populates="payments")

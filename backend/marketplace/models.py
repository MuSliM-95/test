"""
Marketplace feature-local models module.
Defines marketplace-related tables locally and reuses shared metadata.
Other feature code should import from this module.
"""

from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    Text,
    JSON,
    ForeignKey,
)
from sqlalchemy.sql import func

from database.db import (
    database,
    metadata,
    # core refs
    nomenclature,
    prices,
    price_types,
    units,
    categories,
    manufacturers,
    pictures,
    nomenclature_barcodes,
    cboxes,
    warehouse_balances,
    tags,
)


# Marketplace tables (feature-local)
mp_orders = Table(
    "mp_orders",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("customer_order_id", Integer, ForeignKey("customer_orders.id"), nullable=False, index=True),
    Column("customer_order_item_id", Integer, ForeignKey("customer_order_items.id"), nullable=False, index=True),
    Column("seller_order_id", String, unique=True, nullable=False, index=True),
    Column("seller_cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False, index=True),
    Column("product_id", Integer, ForeignKey("nomenclature.id"), nullable=False),
    Column("quantity", Integer, nullable=False, server_default="1"),
    Column("price", Float, nullable=False),
    Column("total_price", Float, nullable=False),
    Column("delivery_type", String, nullable=False),
    Column("delivery_address", String),
    Column("delivery_comment", String),
    Column("delivery_preferred_time", String),
    Column("customer_phone", String, nullable=False),
    Column("customer_lat", Float),
    Column("customer_lon", Float),
    Column("customer_name", String),
    Column("status", String, nullable=False, server_default="pending"),
    Column("assigned_picker_id", Integer, ForeignKey("relation_tg_cashboxes.id")),
    Column("assigned_courier_id", Integer, ForeignKey("relation_tg_cashboxes.id")),
    Column("routing_meta", JSON),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

qr_codes = Table(
    "qr_codes",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("qr_hash", String, unique=True, nullable=False),
    Column("entity_type", String, nullable=False),
    Column("entity_id", Integer, nullable=False),
    Column("salt", String, nullable=False),
    Column("is_active", Boolean, nullable=False, server_default="true"),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

reviews = Table(
    "reviews",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("location_id", Integer, nullable=False),
    Column("phone_hash", String, nullable=False),
    Column("rating", Integer, nullable=False),
    Column("text", Text, nullable=False),
    Column("status", String, nullable=False, server_default="pending"),
    Column("utm", JSON),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

location_rating_aggregates = Table(
    "location_rating_aggregates",
    metadata,
    Column("location_id", Integer, primary_key=True),
    Column("avg_rating", Float, nullable=False),
    Column("reviews_count", Integer, nullable=False),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

favorites = Table(
    "favorites",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("entity_type", String, nullable=False),
    Column("entity_id", Integer, nullable=False),
    Column("phone_hash", String, nullable=False),
    Column("utm", JSON),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

view_events = Table(
    "view_events",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("entity_type", String, nullable=False),
    Column("entity_id", Integer, nullable=False),
    Column("listing_pos", Integer),
    Column("listing_page", Integer),
    Column("phone_hash", String),
    Column("utm", JSON),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
)


__all__ = [
    "database",
    "metadata",
    # shared refs
    "nomenclature",
    "prices",
    "price_types",
    "units",
    "categories",
    "manufacturers",
    "pictures",
    "nomenclature_barcodes",
    "cboxes",
    "warehouse_balances",
    "tags",
    # feature tables
    "mp_orders",
    "qr_codes",
    "reviews",
    "location_rating_aggregates",
    "favorites",
    "view_events",
]



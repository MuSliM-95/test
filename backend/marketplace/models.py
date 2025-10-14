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
    Index
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

# Очередь отзывов для RabbitMQ-архитектуры
review_queue = Table(
    "review_queue",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("cashbox_id", Integer, nullable=False, index=True),
    Column("payload", JSON, nullable=False),
    Column("status", String, nullable=False, server_default="pending"),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("last_error", Text),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Column("processed_at", DateTime(timezone=True)),
    Index("idx_review_queue_status_cashbox", "status", "cashbox_id"),
    Index("idx_review_queue_created", "created_at"),
)

# Универсальная таблица отзывов
reviews = Table(
    "reviews",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("cashbox_id", Integer, nullable=False, index=True),
    Column("review_type", String, nullable=False),  # 'location', 'order', 'item'
    Column("target_id", Integer, nullable=False),
    Column("mp_order_id", Integer, ForeignKey("mp_orders.id"), index=True),
    Column("customer_order_id", Integer, ForeignKey("customer_orders.id"), index=True),
    Column("customer_phone_hash", String, nullable=False, index=True),
    Column("rating", Integer, nullable=False),
    Column("text", Text, nullable=False),
    Column("status", String, nullable=False, server_default="pending"),
    Column("source_data", JSON),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
    Index("idx_reviews_cashbox_type_target", "cashbox_id", "review_type", "target_id"),
    Index("idx_reviews_phone_created", "customer_phone_hash", "created_at"),
)

# Детальные отзывы на товары в заказе
item_reviews = Table(
    "item_reviews",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("review_id", Integer, ForeignKey("reviews.id"), nullable=False, index=True),
    Column("customer_order_item_id", Integer, ForeignKey("customer_order_items.id"), nullable=False, index=True),
    Column("product_id", Integer, ForeignKey("nomenclature.id"), nullable=False, index=True),
    Column("rating", Integer, nullable=False),
    Column("text", Text),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Index("idx_item_reviews_product", "product_id", "rating"),
    Index("idx_item_reviews_review", "review_id", "customer_order_item_id"),
)

# Агрегаты рейтингов по кассам и сущностям
cashbox_rating_aggregates = Table(
    "cashbox_rating_aggregates",
    metadata,
    Column("cashbox_id", Integer, primary_key=True),
    Column("entity_type", String, primary_key=True),  # 'location', 'product', 'order'
    Column("entity_id", Integer, primary_key=True),
    Column("avg_rating", Float, nullable=False),
    Column("reviews_count", Integer, nullable=False),
    Column("last_review_id", Integer, ForeignKey("reviews.id")),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
    Index("idx_rating_agg_cashbox_entity", "cashbox_id", "entity_type", "entity_id"),
    Index("idx_rating_agg_rating", "avg_rating", "reviews_count"),
)

# Агрегаты рейтингов для товаров (номенклатуры)
item_rating_aggregates = Table(
    "item_rating_aggregates",
    metadata,
    Column("product_id", Integer, ForeignKey("nomenclature.id"), primary_key=True),
    Column("avg_rating", Float, nullable=False),
    Column("reviews_count", Integer, nullable=False),
    Column("last_review_id", Integer, ForeignKey("reviews.id")),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
    Index("idx_item_agg_rating", "avg_rating", "reviews_count"),
)

# Старая таблица для обратной совместимости (можно удалить после миграции)
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
    Index("idx_favorites_phone_entity", "phone_hash", "entity_type", "entity_id"),
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
    Index("idx_view_events_entity", "entity_type", "entity_id", "created_at"),
    Index("idx_view_events_phone", "phone_hash", "created_at"),
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
    "review_queue",
    "reviews",
    "item_reviews",
    "cashbox_rating_aggregates",
    "item_rating_aggregates",
    "location_rating_aggregates",
    "favorites",
    "view_events",
]
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
    # Основные идентификаторы
    Column("id", Integer, primary_key=True, index=True),  # Уникальный ID заказа маркетплейса
    Column("customer_order_id", Integer, ForeignKey("customer_orders.id"), nullable=False, index=True),  # Ссылка на заказ клиента
    Column("customer_order_item_id", Integer, ForeignKey("customer_order_items.id"), nullable=False, index=True),  # Ссылка на позицию заказа клиента
    Column("seller_order_id", String, unique=True, nullable=False, index=True),  # Уникальный ID заказа у продавца (генерируется системой)
    Column("seller_cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False, index=True),  # ID кассы продавца
    
    # Информация о товаре
    Column("product_id", Integer, ForeignKey("nomenclature.id"), nullable=False),  # ID товара из номенклатуры
    Column("quantity", Integer, nullable=False, server_default="1"),  # Количество товара
    Column("price", Float, nullable=False),  # Цена за единицу товара
    Column("total_price", Float, nullable=False),  # Общая стоимость заказа
    
    # Информация о доставке
    Column("delivery_type", String, nullable=False),  # Тип доставки (pickup, delivery, etc.)
    Column("delivery_address", String),  # Адрес доставки
    Column("delivery_comment", String),  # Комментарий к доставке
    Column("delivery_preferred_time", String),  # Предпочтительное время доставки
    
    # Информация о заказчике (кто делает заказ)
    Column("customer_phone", String, nullable=False),  # Телефон заказчика
    Column("customer_lat", Float),  # Широта местоположения заказчика
    Column("customer_lon", Float),  # Долгота местоположения заказчика
    Column("customer_name", String),  # Имя заказчика
    
    # Информация о получателе (кто получает заказ)
    Column("recipient_phone", String),  # Телефон получателя (может отличаться от заказчика)
    Column("recipient_name", String),  # Имя получателя
    Column("recipient_lat", Float),  # Широта места получения
    Column("recipient_lon", Float),  # Долгота места получения
    Column("order_type", String, nullable=False, server_default="self"),  # Тип заказа: self (сам), other (для другого), corporate (корпоративный)
    
    # Статус и назначения
    Column("status", String, nullable=False, server_default="pending"),  # Статус заказа (pending, processing, completed, cancelled)
    Column("assigned_picker_id", Integer, ForeignKey("relation_tg_cashboxes.id")),  # ID назначенного сборщика
    Column("assigned_courier_id", Integer, ForeignKey("relation_tg_cashboxes.id")),  # ID назначенного курьера
    
    # Метаданные
    Column("routing_meta", JSON),  # Метаданные для маршрутизации и логистики
    
    # Временные метки
    Column("created_at", DateTime(timezone=True), server_default=func.now()),  # Время создания заказа
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),  # Время последнего обновления
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



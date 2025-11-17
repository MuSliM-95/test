"""Подключение к базе данных и определение таблиц"""
import os

from databases import Database
from sqlalchemy import (Boolean, Column, DateTime, ForeignKey, Integer,
                        MetaData, String, Table, create_engine)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@localhost:5432/marketplace"
)

# Экземпляр базы данных
database = Database(DATABASE_URL)

# Метаданные SQLAlchemy
metadata = MetaData()

# Таблица глобальных категорий
global_categories = Table(
    "global_categories",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(255), nullable=False),
    Column("description", String),
    Column("code", Integer),
    Column("parent_id", Integer, ForeignKey("global_categories.id")),
    Column("external_id", String(255)),
    Column("image_url", String(500)),
    Column("is_active", Boolean, default=True),
    Column("created_at", DateTime, server_default="now()"),
    Column(
        "updated_at",
        DateTime,
        server_default="now()",
        onupdate="now()"
    ),
)

# Движок для создания таблиц (если нужно)
engine = create_engine(DATABASE_URL)

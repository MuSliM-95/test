from datetime import datetime

import databases
from sqlalchemy import (Boolean, Column, DateTime, Integer, MetaData, String,
                        Table)

import os
DATABASE_URL = (
    f"postgresql://{os.environ.get('POSTGRES_USER', 'example')}:{os.environ.get('POSTGRES_PASS', 'example')}"
    f"@{os.environ.get('POSTGRES_HOST', 'postgres-bridge')}:{os.environ.get('POSTGRES_PORT', '5432')}/cash_2"
)

database = databases.Database(DATABASE_URL)
metadata = MetaData()

global_categories = Table(
    "global_categories",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("description", String),
    Column("code", Integer),
    Column("parent_id", Integer),
    Column("external_id", String),
    Column("image_url", String),
    Column("is_active", Boolean, default=True),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column(
        "updated_at",
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    ),
)

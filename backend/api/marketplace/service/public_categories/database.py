from datetime import datetime

import databases
from sqlalchemy import (Boolean, Column, DateTime, Integer, MetaData, String,
                        Table)

import os
from dotenv import load_dotenv
load_dotenv()
POSTGRES_USER = os.getenv('POSTGRES_USER', 'example')
POSTGRES_PASS = os.getenv('POSTGRES_PASS', 'example')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-bridge')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'cash_2')

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
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

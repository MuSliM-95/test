"""add_qr_codes_table

Revision ID: 915d2c3e6554
Revises: beeaac81d079
Create Date: 2025-10-11 00:30:39.013535

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '915d2c3e6554'
down_revision = 'beeaac81d079'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Создаем таблицу QR-кодов
    op.create_table('qr_codes',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('qr_hash', sa.String(), nullable=False, unique=True),
        sa.Column('entity_type', sa.String(), nullable=False),  # 'product' или 'location'
        sa.Column('entity_id', sa.Integer(), nullable=False),
        sa.Column('salt', sa.String(), nullable=False),  # Соль для генерации хэша
        sa.Column('is_active', sa.Boolean(), nullable=False, default=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('qr_hash')
    )
    
    # Создаем индексы для оптимизации запросов
    op.create_index('idx_qr_codes_hash', 'qr_codes', ['qr_hash'])
    op.create_index('idx_qr_codes_entity', 'qr_codes', ['entity_type', 'entity_id'])
    op.create_index('idx_qr_codes_active', 'qr_codes', ['is_active'])


def downgrade() -> None:
    op.drop_table('qr_codes')

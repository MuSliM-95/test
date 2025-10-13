"""add_favorites_table

Revision ID: e5614f22c878
Revises: aaf5df1407bc
Create Date: 2025-10-11 01:05:40.535988

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e5614f22c878'
down_revision = 'aaf5df1407bc'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Создаем таблицу избранного
    op.create_table('favorites',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('entity_type', sa.String(), nullable=False),  # 'product' или 'location'
        sa.Column('entity_id', sa.Integer(), nullable=False),
        sa.Column('phone_hash', sa.String(), nullable=False),  # Хэш телефона для анонимности
        sa.Column('utm', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Создаем индексы для оптимизации запросов
    op.create_index('idx_favorites_phone_hash', 'favorites', ['phone_hash'])
    op.create_index('idx_favorites_entity', 'favorites', ['entity_type', 'entity_id'])
    op.create_index('idx_favorites_created_at', 'favorites', ['created_at'])
    
    # Уникальный индекс для предотвращения дублирования
    op.create_index('idx_favorites_unique', 'favorites', ['phone_hash', 'entity_type', 'entity_id'], unique=True)


def downgrade() -> None:
    op.drop_table('favorites')

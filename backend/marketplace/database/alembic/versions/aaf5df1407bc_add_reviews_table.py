"""add_reviews_table

Revision ID: aaf5df1407bc
Revises: 915d2c3e6554
Create Date: 2025-10-11 00:43:48.467236

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aaf5df1407bc'
down_revision = '915d2c3e6554'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Создаем таблицу отзывов
    op.create_table('reviews',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('location_id', sa.Integer(), nullable=False),
        sa.Column('phone_hash', sa.String(), nullable=False),  # Хэш телефона для анонимности
        sa.Column('rating', sa.Integer(), nullable=False),  # 1-5
        sa.Column('text', sa.Text(), nullable=False),
        sa.Column('status', sa.String(), nullable=False, default='pending'),  # pending, visible, hidden
        sa.Column('utm', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Создаем индексы для оптимизации запросов
    op.create_index('idx_reviews_location_id', 'reviews', ['location_id'])
    op.create_index('idx_reviews_status', 'reviews', ['status'])
    op.create_index('idx_reviews_rating', 'reviews', ['rating'])
    op.create_index('idx_reviews_created_at', 'reviews', ['created_at'])
    
    # Создаем таблицу агрегатов рейтингов для быстрого доступа
    op.create_table('location_rating_aggregates',
        sa.Column('location_id', sa.Integer(), nullable=False),
        sa.Column('avg_rating', sa.Float(), nullable=False),
        sa.Column('reviews_count', sa.Integer(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('location_id')
    )


def downgrade() -> None:
    op.drop_table('location_rating_aggregates')
    op.drop_table('reviews')

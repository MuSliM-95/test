"""add_marketplace_orders_table

Revision ID: beeaac81d079
Revises: 75bffef51bdf
Create Date: 2025-10-11 00:05:10.180052

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'beeaac81d079'
down_revision = '75bffef51bdf'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Создаем таблицу заказов маркетплейса
    op.create_table('mp_orders',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('order_id', sa.String(), nullable=False, unique=True),
        sa.Column('product_id', sa.Integer(), nullable=False),
        sa.Column('listing_pos', sa.Integer(), nullable=True),
        sa.Column('listing_page', sa.Integer(), nullable=True),
        sa.Column('location_id', sa.Integer(), nullable=True),
        sa.Column('utm', sa.JSON(), nullable=True),
        sa.Column('delivery_type', sa.String(), nullable=False),
        sa.Column('delivery_address', sa.String(), nullable=True),
        sa.Column('delivery_comment', sa.String(), nullable=True),
        sa.Column('delivery_preferred_time', sa.String(), nullable=True),
        sa.Column('customer_phone', sa.String(), nullable=False),
        sa.Column('customer_lat', sa.Float(), nullable=True),
        sa.Column('customer_lon', sa.Float(), nullable=True),
        sa.Column('customer_name', sa.String(), nullable=True),
        sa.Column('quantity', sa.Integer(), nullable=False, default=1),
        sa.Column('status', sa.String(), nullable=False, default='pending'),
        sa.Column('routing_meta', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('order_id')
    )
    
    # Создаем индексы для оптимизации запросов
    op.create_index('idx_mp_orders_status', 'mp_orders', ['status'])
    op.create_index('idx_mp_orders_customer_phone', 'mp_orders', ['customer_phone'])
    op.create_index('idx_mp_orders_created_at', 'mp_orders', ['created_at'])


def downgrade() -> None:
    op.drop_table('mp_orders')

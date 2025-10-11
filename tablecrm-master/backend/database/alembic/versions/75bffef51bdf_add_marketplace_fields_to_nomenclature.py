"""add_marketplace_fields_to_nomenclature

Revision ID: 75bffef51bdf
Revises: b3d8b5761180
Create Date: 2025-10-10 23:05:56.779989

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '75bffef51bdf'
down_revision = 'b3d8b5761180'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем поля для маркетплейса в таблицу nomenclature
    op.add_column('nomenclature', sa.Column('public', sa.Boolean(), nullable=True, server_default='false'))
    op.add_column('nomenclature', sa.Column('geo_point', sa.String(), nullable=True))
    op.add_column('nomenclature', sa.Column('city', sa.String(100), nullable=True))
    
    # Создаем индексы для оптимизации запросов
    op.create_index('idx_nomenclature_public', 'nomenclature', ['public'])
    op.create_index('idx_nomenclature_city', 'nomenclature', ['city'])
    
    # Добавляем поля для маркетплейса в таблицу cashboxes (для локаций)
    op.add_column('cashboxes', sa.Column('public', sa.Boolean(), nullable=True, server_default='false'))
    op.add_column('cashboxes', sa.Column('geo_point', sa.String(), nullable=True))
    op.add_column('cashboxes', sa.Column('city', sa.String(100), nullable=True))
    
    # Создаем индексы для cashboxes
    op.create_index('idx_cashboxes_public', 'cashboxes', ['public'])
    op.create_index('idx_cashboxes_city', 'cashboxes', ['city'])


def downgrade() -> None:
    # Удаляем индексы
    op.drop_index('idx_cashboxes_city', table_name='cashboxes')
    op.drop_index('idx_cashboxes_public', table_name='cashboxes')
    op.drop_index('idx_nomenclature_city', table_name='nomenclature')
    op.drop_index('idx_nomenclature_public', table_name='nomenclature')
    
    # Удаляем поля из cashboxes
    op.drop_column('cashboxes', 'city')
    op.drop_column('cashboxes', 'geo_point')
    op.drop_column('cashboxes', 'public')
    
    # Удаляем поля из nomenclature
    op.drop_column('nomenclature', 'city')
    op.drop_column('nomenclature', 'geo_point')
    op.drop_column('nomenclature', 'public')

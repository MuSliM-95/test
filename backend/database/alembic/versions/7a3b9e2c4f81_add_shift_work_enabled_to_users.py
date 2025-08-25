"""add_shift_work_enabled_to_users

Revision ID: 7a3b9e2c4f81
Revises: c4f5d6e789ab
Create Date: 2025-08-25 13:20:57.0210000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '7a3b9e2c4f81'
down_revision = 'c4f5d6e789ab'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('relation_tg_cashboxes', 
                  sa.Column('shift_work_enabled', sa.Boolean, 
                           nullable=True, server_default='false'))
    
    op.execute("UPDATE relation_tg_cashboxes SET shift_work_enabled = false WHERE shift_work_enabled IS NULL")
    
    # Убираем nullable после установки значений
    op.alter_column('relation_tg_cashboxes', 'shift_work_enabled', nullable=False)


def downgrade():
    op.drop_column('relation_tg_cashboxes', 'shift_work_enabled')

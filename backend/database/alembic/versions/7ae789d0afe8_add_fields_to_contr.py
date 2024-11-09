"""add fields to contr

Revision ID: 7ae789d0afe8
Revises: fab805a9d974
Create Date: 2024-11-09 23:23:29.060643

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '7ae789d0afe8'
down_revision = 'dbbe06a4083d'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('contragents', sa.Column('additional_phones', sa.String(), nullable=True))
    op.add_column('contragents', sa.Column('gender', sa.Enum('male', 'female', name='gender'), nullable=True))


def downgrade() -> None:
    pass

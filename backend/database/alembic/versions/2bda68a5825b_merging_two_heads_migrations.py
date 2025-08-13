"""merging-two-heads-migrations

Revision ID: 2bda68a5825b
Revises: ded27e5f734b, 9c121f40e783
Create Date: 2025-08-13 14:16:12.347790

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2bda68a5825b'
down_revision = ('ded27e5f734b', '9c121f40e783')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

"""add event field in marketplace_view_events

Revision ID: 813d08d49474
Revises: f0b3d06a3530
Create Date: 2025-12-02 00:50:03.237679

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '813d08d49474'
down_revision = 'f0b3d06a3530'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "marketplace_view_events",
        sa.Column("view_type", sa.String(), nullable=False, server_default="catalog")
    )


def downgrade() -> None:
    pass

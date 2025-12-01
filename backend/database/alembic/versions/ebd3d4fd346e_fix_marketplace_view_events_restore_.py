"""fix marketplace_view_events: restore event column

Revision ID: ebd3d4fd346e
Revises: 813d08d49474
Create Date: 2025-12-02 01:39:54.930704

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ebd3d4fd346e'
down_revision = '813d08d49474'
branch_labels = None
depends_on = None


def upgrade() -> None:
    try:
        op.drop_column("marketplace_view_events", "view_type")
    except Exception:
        pass

    op.add_column(
        "marketplace_view_events",
        sa.Column("event", sa.String(), nullable=False, server_default="view")
    )


def downgrade():
    op.drop_column("marketplace_view_events", "event")
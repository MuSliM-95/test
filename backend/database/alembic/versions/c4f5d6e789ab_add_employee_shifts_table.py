"""add_employee_shifts_table

Revision ID: c4f5d6e789ab
Revises: f4d18b4db9a1
Create Date: 2025-8-22 15:23:47.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'c4f5d6e789ab'
down_revision = '1c68f42fc047'
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()

    result = connection.execute(sa.text("""
        SELECT 1 FROM pg_type t 
        JOIN pg_namespace n ON n.oid = t.typnamespace  
        WHERE t.typname = 'shiftstatus' AND n.nspname = 'public'
    """))
    
    if not result.fetchone():
        shiftstatus_enum = postgresql.ENUM('on_shift', 'off_shift', 'on_break', name='shiftstatus')
        shiftstatus_enum.create(connection)
    
    op.create_table('employee_shifts',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('cashbox_id', sa.Integer(), nullable=False),
        sa.Column('shift_start', sa.DateTime(), nullable=False),
        sa.Column('shift_end', sa.DateTime(), nullable=True),
        sa.Column('status', postgresql.ENUM('on_shift', 'off_shift', 'on_break', name='shiftstatus'), nullable=False, server_default='off_shift'),
        sa.Column('break_start', sa.DateTime(), nullable=True),
        sa.Column('break_duration', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['cashbox_id'], ['cashboxes.id'], ),
        sa.ForeignKeyConstraint(['user_id'], ['relation_tg_cashboxes.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    
    op.create_index('ix_employee_shifts_id', 'employee_shifts', ['id'], unique=False)
    op.create_index('ix_employee_shifts_user_id', 'employee_shifts', ['user_id'], unique=False)
    op.create_index('ix_employee_shifts_status', 'employee_shifts', ['status'], unique=False)
    op.create_index('ix_employee_shifts_cashbox_id', 'employee_shifts', ['cashbox_id'], unique=False)


def downgrade():
    op.drop_index('ix_employee_shifts_cashbox_id', table_name='employee_shifts')
    op.drop_index('ix_employee_shifts_status', table_name='employee_shifts')
    op.drop_index('ix_employee_shifts_user_id', table_name='employee_shifts')
    op.drop_index('ix_employee_shifts_id', table_name='employee_shifts')
    
    op.drop_table('employee_shifts')
    
    connection = op.get_bind()
    result = connection.execute(sa.text("""
        SELECT 1 FROM information_schema.columns 
        WHERE udt_name = 'shiftstatus' AND table_name != 'employee_shifts'
    """))
    if not result.fetchone():
        shiftstatus_enum = postgresql.ENUM('on_shift', 'off_shift', 'on_break', name='shiftstatus')
        shiftstatus_enum.drop(connection)

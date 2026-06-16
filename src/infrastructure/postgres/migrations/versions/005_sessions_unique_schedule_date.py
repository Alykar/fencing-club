"""Add unique constraint (schedule_id, starts_at) on training_sessions

Revision ID: 005
Revises: 004
Create Date: 2026-06-16
"""

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None

from alembic import op


def upgrade() -> None:
    # Remove duplicate rows before adding constraint (keep the first inserted)
    op.execute("""
        DELETE FROM training_sessions
        WHERE id NOT IN (
            SELECT DISTINCT ON (schedule_id, starts_at) id
            FROM training_sessions
            WHERE schedule_id IS NOT NULL
            ORDER BY schedule_id, starts_at, created_at
        )
        AND schedule_id IS NOT NULL
    """)
    op.execute("""
        ALTER TABLE training_sessions
        ADD CONSTRAINT uq_sessions_schedule_starts
        UNIQUE (schedule_id, starts_at)
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE training_sessions
        DROP CONSTRAINT IF EXISTS uq_sessions_schedule_starts
    """)

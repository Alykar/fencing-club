"""Add description to schedules/sessions; nullable populate_to; batch save sessions

Revision ID: 003
Revises: 002
Create Date: 2026-06-16
"""

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None

from alembic import op


def upgrade() -> None:
    op.execute("ALTER TABLE training_schedules ADD COLUMN IF NOT EXISTS description TEXT")
    op.execute("ALTER TABLE training_schedules ALTER COLUMN populate_to DROP NOT NULL")
    op.execute("ALTER TABLE training_schedules ALTER COLUMN kind SET DEFAULT 'other'")

    op.execute("ALTER TABLE training_sessions ADD COLUMN IF NOT EXISTS description TEXT")
    op.execute("ALTER TABLE training_sessions ALTER COLUMN kind SET DEFAULT 'other'")


def downgrade() -> None:
    op.execute("ALTER TABLE training_schedules DROP COLUMN IF EXISTS description")
    op.execute("ALTER TABLE training_sessions DROP COLUMN IF EXISTS description")

"""Add left_elo_before / right_elo_before to rating_matches

Revision ID: 004
Revises: 003
Create Date: 2026-06-16
"""

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None

from alembic import op


def upgrade() -> None:
    op.execute("ALTER TABLE rating_matches ADD COLUMN IF NOT EXISTS left_elo_before  FLOAT")
    op.execute("ALTER TABLE rating_matches ADD COLUMN IF NOT EXISTS right_elo_before FLOAT")


def downgrade() -> None:
    op.execute("ALTER TABLE rating_matches DROP COLUMN IF EXISTS left_elo_before")
    op.execute("ALTER TABLE rating_matches DROP COLUMN IF EXISTS right_elo_before")

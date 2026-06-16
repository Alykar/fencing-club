"""Separate hall pass from system access.

Previously is_access_granted controlled both login and hall pass.
Now:
  - is_access_granted = True for all users (login is immediate after registration)
  - has_hall_pass = False by default (admin grants separately)

Revision ID: 006
Revises: 005
Create Date: 2026-06-16
"""

revision = "006"
down_revision = "005"

from alembic import op  # noqa: E402


def upgrade() -> None:
    # Add has_hall_pass column: copy value from old is_access_granted
    # so existing users who had system access keep their hall pass.
    op.execute("""
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS has_hall_pass BOOLEAN NOT NULL DEFAULT FALSE
    """)
    op.execute("""
        UPDATE users SET has_hall_pass = is_access_granted
    """)
    # Grant system access to everyone — login is now immediate.
    op.execute("""
        UPDATE users SET is_access_granted = TRUE
    """)


def downgrade() -> None:
    # Restore old behavior: system access = hall pass
    op.execute("""
        UPDATE users SET is_access_granted = has_hall_pass
    """)
    op.execute("ALTER TABLE users DROP COLUMN IF EXISTS has_hall_pass")

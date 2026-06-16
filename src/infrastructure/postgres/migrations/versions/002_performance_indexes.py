"""Performance indexes + pg_trgm for search

Revision ID: 002
Revises: 001
Create Date: 2026-06-16
"""

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None

from alembic import op


def upgrade() -> None:
    # pg_trgm — enables GIN indexes for ILIKE/trigram search
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # ── users ────────────────────────────────────────────────────────────────
    # list_all() ORDER BY created_at DESC
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_users_created_at_desc "
        "ON users (created_at DESC)"
    )
    # list_pending_access() WHERE is_access_granted = FALSE ORDER BY created_at ASC
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_users_pending_created "
        "ON users (created_at ASC) WHERE is_access_granted = FALSE"
    )
    # search() ILIKE :pattern — trigram GIN
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_users_name_trgm "
        "ON users USING gin (name gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_users_email_trgm "
        "ON users USING gin (email gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_users_tel_trgm "
        "ON users USING gin (tel gin_trgm_ops)"
    )

    # ── training_schedules ───────────────────────────────────────────────────
    # list_active() WHERE status = 'active' ORDER BY time_of_day
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_schedules_active_time "
        "ON training_schedules (time_of_day) WHERE status = 'active'"
    )

    # ── attendances ──────────────────────────────────────────────────────────
    # list_by_user / count_by_user_in_range — FK + range
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendances_user_created "
        "ON attendances (user_id, created_at DESC)"
    )
    # list_by_session — used in list_session_attendance
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendances_session "
        "ON attendances (training_session_id)"
    )

    # ── rating_bouts ─────────────────────────────────────────────────────────
    # get_match_by_id fetches bouts WHERE match_id = $1 ORDER BY bout_number
    # UNIQUE(match_id, bout_number) already exists but no plain FK index
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_rating_bouts_match "
        "ON rating_bouts (match_id, bout_number)"
    )

    # ── rating_passes ────────────────────────────────────────────────────────
    # get_match_by_id fetches passes WHERE bout_id = ANY($1) — batch
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_rating_passes_bout "
        "ON rating_passes (bout_id, pass_number)"
    )

    # ── rating_matches ───────────────────────────────────────────────────────
    # Existing composite idx_rating_matches_users(left, right) is poor for
    # OR-query (left_user_id = $1 OR right_user_id = $1). Add separate indexes.
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_rating_matches_left_created "
        "ON rating_matches (left_user_id, created_at DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_rating_matches_right_created "
        "ON rating_matches (right_user_id, created_at DESC)"
    )

    # ── user_ratings ─────────────────────────────────────────────────────────
    # list_leaderboard() ORDER BY elo DESC WHERE matches_played >= 10
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_ratings_leaderboard "
        "ON user_ratings (elo DESC) WHERE matches_played >= 10"
    )
    # get_by_user_id
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_ratings_user "
        "ON user_ratings (user_id)"
    )


def downgrade() -> None:
    for idx in (
        "idx_users_created_at_desc",
        "idx_users_pending_created",
        "idx_users_name_trgm",
        "idx_users_email_trgm",
        "idx_users_tel_trgm",
        "idx_schedules_active_time",
        "idx_attendances_user_created",
        "idx_attendances_session",
        "idx_rating_bouts_match",
        "idx_rating_passes_bout",
        "idx_rating_matches_left_created",
        "idx_rating_matches_right_created",
        "idx_user_ratings_leaderboard",
        "idx_user_ratings_user",
    ):
        op.execute(f"DROP INDEX IF EXISTS {idx}")

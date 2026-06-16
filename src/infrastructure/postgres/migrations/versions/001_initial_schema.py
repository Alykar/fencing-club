"""Initial schema

Revision ID: 001
Revises:
Create Date: 2026-01-01

"""
revision = "001"
down_revision = None
branch_labels = None
depends_on = None

from alembic import op


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
    op.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            name              TEXT NOT NULL,
            tel               TEXT NOT NULL,
            email             TEXT NOT NULL UNIQUE,
            tg                TEXT,
            vk                TEXT,
            role              TEXT NOT NULL DEFAULT 'user',
            password_hash     TEXT NOT NULL,
            is_access_granted BOOLEAN NOT NULL DEFAULT FALSE,
            is_blocked        BOOLEAN NOT NULL DEFAULT FALSE,
            referral_source   TEXT NOT NULL DEFAULT 'other',
            friend_name       TEXT,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_users_access ON users(is_access_granted)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS training_schedules (
            id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            title            TEXT NOT NULL,
            kind             TEXT NOT NULL,
            status           TEXT NOT NULL DEFAULT 'active',
            weekdays         TEXT[] NOT NULL DEFAULT '{}',
            time_of_day      TIME NOT NULL,
            duration_minutes INTEGER NOT NULL DEFAULT 90,
            location         TEXT NOT NULL DEFAULT '',
            instructor_id    UUID REFERENCES users(id) ON DELETE SET NULL,
            cost_per_session NUMERIC(10,2),
            populate_from    DATE NOT NULL,
            populate_to      DATE NOT NULL,
            created_by       UUID NOT NULL REFERENCES users(id),
            created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS training_sessions (
            id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            title            TEXT NOT NULL,
            kind             TEXT NOT NULL,
            starts_at        TIMESTAMPTZ NOT NULL,
            duration_minutes INTEGER NOT NULL DEFAULT 90,
            location         TEXT NOT NULL DEFAULT '',
            instructor_id    UUID REFERENCES users(id) ON DELETE SET NULL,
            schedule_id      UUID REFERENCES training_schedules(id) ON DELETE SET NULL,
            created_by       UUID NOT NULL REFERENCES users(id),
            updated_by       UUID NOT NULL REFERENCES users(id),
            created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_sessions_starts_at ON training_sessions(starts_at)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS attendances (
            training_session_id UUID NOT NULL REFERENCES training_sessions(id) ON DELETE CASCADE,
            user_id             UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_by          UUID NOT NULL REFERENCES users(id),
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (training_session_id, user_id)
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_id      UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            amount       NUMERIC(10,2) NOT NULL,
            paid_at      DATE NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_one_time  BOOLEAN NOT NULL DEFAULT FALSE,
            note         TEXT,
            created_by   UUID NOT NULL REFERENCES users(id),
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id, paid_at DESC)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS weapon_types (
            id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            name       TEXT NOT NULL UNIQUE,
            is_default BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)
    op.execute("""
        INSERT INTO weapon_types (name, is_default) VALUES
            ('Длинный меч', TRUE),
            ('Военная сабля', TRUE),
            ('Рапира', FALSE)
        ON CONFLICT (name) DO NOTHING
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            title            TEXT NOT NULL DEFAULT '',
            left_label       TEXT NOT NULL,
            right_label      TEXT NOT NULL,
            max_score        INTEGER,
            duration_seconds INTEGER,
            status           TEXT NOT NULL DEFAULT 'ongoing',
            left_score       INTEGER NOT NULL DEFAULT 0,
            right_score      INTEGER NOT NULL DEFAULT 0,
            winner           TEXT,
            created_by       UUID NOT NULL REFERENCES users(id),
            created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS rating_matches (
            id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            left_user_id    UUID NOT NULL REFERENCES users(id),
            right_user_id   UUID NOT NULL REFERENCES users(id),
            weapon_type_ids UUID[] NOT NULL DEFAULT '{}',
            status          TEXT NOT NULL DEFAULT 'ongoing',
            winner_user_id  UUID REFERENCES users(id),
            created_by      UUID NOT NULL REFERENCES users(id),
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_rating_matches_users ON rating_matches(left_user_id, right_user_id)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS rating_bouts (
            id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            match_id       UUID NOT NULL REFERENCES rating_matches(id) ON DELETE CASCADE,
            bout_number    INTEGER NOT NULL,
            winner_user_id UUID REFERENCES users(id),
            UNIQUE (match_id, bout_number)
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS rating_passes (
            id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            bout_id        UUID NOT NULL REFERENCES rating_bouts(id) ON DELETE CASCADE,
            pass_number    INTEGER NOT NULL,
            winner_user_id UUID NOT NULL REFERENCES users(id),
            UNIQUE (bout_id, pass_number)
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS user_ratings (
            user_id        UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            elo            NUMERIC(10,2) NOT NULL DEFAULT 1000,
            matches_played INTEGER NOT NULL DEFAULT 0
        )
    """)


def downgrade() -> None:
    for table in [
        "user_ratings", "rating_passes", "rating_bouts", "rating_matches",
        "matches", "weapon_types", "payments", "attendances",
        "training_sessions", "training_schedules", "users",
    ]:
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

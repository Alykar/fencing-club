from __future__ import annotations

from datetime import datetime
from uuid import UUID

import asyncpg

from domain.entities.enums.training_kind import TrainingKind
from domain.entities.training_session import TrainingSession
from domain.ports.repositories.training_sessions import TrainingSessionsRepo
from infrastructure.postgres.db import PostgresDB


def _row_to_session(row: asyncpg.Record) -> TrainingSession:
    return TrainingSession(
        id=row["id"],
        title=row["title"],
        description=row["description"],
        kind=TrainingKind(row["kind"]),
        starts_at=row["starts_at"],
        duration_minutes=row["duration_minutes"],
        location=row["location"],
        instructor_id=row["instructor_id"],
        schedule_id=row["schedule_id"],
        created_by=row["created_by"],
        updated_by=row["updated_by"],
        created_at=row["created_at"],
    )


class PostgresTrainingSessionsRepo(TrainingSessionsRepo):
    def __init__(self, db: PostgresDB) -> None:
        self._db = db

    async def save(self, session: TrainingSession) -> None:
        await self._db.execute(
            """
            INSERT INTO training_sessions (
                id, title, description, kind, starts_at, duration_minutes, location,
                instructor_id, schedule_id, created_by, updated_by, created_at
            ) VALUES (
                :id, :title, :description, :kind, :starts_at, :duration_minutes, :location,
                :instructor_id, :schedule_id, :created_by, :updated_by, :created_at
            )
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                kind = EXCLUDED.kind,
                starts_at = EXCLUDED.starts_at,
                duration_minutes = EXCLUDED.duration_minutes,
                location = EXCLUDED.location,
                instructor_id = EXCLUDED.instructor_id,
                updated_by = EXCLUDED.updated_by
            """,
            {
                "id": str(session.id),
                "title": session.title,
                "description": session.description,
                "kind": session.kind.value,
                "starts_at": session.starts_at,
                "duration_minutes": session.duration_minutes,
                "location": session.location,
                "instructor_id": str(session.instructor_id) if session.instructor_id else None,
                "schedule_id": str(session.schedule_id) if session.schedule_id else None,
                "created_by": str(session.created_by),
                "updated_by": str(session.updated_by),
                "created_at": session.created_at,
            },
        )

    async def save_batch(self, sessions: list[TrainingSession]) -> None:
        """Bulk-insert sessions, silently skip if (schedule_id, starts_at) already exists."""
        if not sessions:
            return
        for s in sessions:
            await self._db.execute(
                """
                INSERT INTO training_sessions (
                    id, title, description, kind, starts_at, duration_minutes, location,
                    instructor_id, schedule_id, created_by, updated_by, created_at
                ) VALUES (
                    :id, :title, :description, :kind, :starts_at, :duration_minutes, :location,
                    :instructor_id, :schedule_id, :created_by, :updated_by, :created_at
                )
                ON CONFLICT (schedule_id, starts_at) DO NOTHING
                """,
                {
                    "id": str(s.id),
                    "title": s.title,
                    "description": s.description,
                    "kind": s.kind.value,
                    "starts_at": s.starts_at,
                    "duration_minutes": s.duration_minutes,
                    "location": s.location,
                    "instructor_id": str(s.instructor_id) if s.instructor_id else None,
                    "schedule_id": str(s.schedule_id) if s.schedule_id else None,
                    "created_by": str(s.created_by),
                    "updated_by": str(s.updated_by),
                    "created_at": s.created_at,
                },
            )

    async def get_by_id(self, session_id: UUID) -> TrainingSession | None:
        row = await self._db.fetchrow(
            "SELECT * FROM training_sessions WHERE id = :id", {"id": str(session_id)}
        )
        return _row_to_session(row) if row else None

    async def list_by_date_range(self, from_dt: datetime, to_dt: datetime) -> list[TrainingSession]:
        rows = await self._db.fetch(
            """
            SELECT * FROM training_sessions
            WHERE starts_at >= :from_dt AND starts_at < :to_dt
            ORDER BY starts_at
            """,
            {"from_dt": from_dt, "to_dt": to_dt},
        )
        return [_row_to_session(r) for r in rows]

    async def list_upcoming(self, limit: int = 10) -> list[TrainingSession]:
        rows = await self._db.fetch(
            """
            SELECT * FROM training_sessions
            WHERE starts_at >= NOW()
            ORDER BY starts_at
            LIMIT :limit
            """,
            {"limit": limit},
        )
        return [_row_to_session(r) for r in rows]

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import asyncpg

from domain.entities.attendance import Attendance
from domain.ports.repositories.attendances import AttendancesRepo
from infrastructure.postgres.db import PostgresDB


def _row_to_attendance(row: asyncpg.Record) -> Attendance:
    return Attendance(
        training_session_id=row["training_session_id"],
        user_id=row["user_id"],
        created_by=row["created_by"],
        created_at=row["created_at"],
    )


class PostgresAttendancesRepo(AttendancesRepo):
    def __init__(self, db: PostgresDB) -> None:
        self._db = db

    async def save(self, attendance: Attendance) -> None:
        await self._db.execute(
            """
            INSERT INTO attendances (training_session_id, user_id, created_by, created_at)
            VALUES (:session_id, :user_id, :created_by, :created_at)
            ON CONFLICT (training_session_id, user_id) DO NOTHING
            """,
            {
                "session_id": str(attendance.training_session_id),
                "user_id": str(attendance.user_id),
                "created_by": str(attendance.created_by),
                "created_at": attendance.created_at,
            },
        )

    async def delete(self, training_session_id: UUID, user_id: UUID) -> None:
        await self._db.execute(
            "DELETE FROM attendances WHERE training_session_id = :session_id AND user_id = :user_id",
            {"session_id": str(training_session_id), "user_id": str(user_id)},
        )

    async def get(self, training_session_id: UUID, user_id: UUID) -> Attendance | None:
        row = await self._db.fetchrow(
            "SELECT * FROM attendances WHERE training_session_id = :session_id AND user_id = :user_id",
            {"session_id": str(training_session_id), "user_id": str(user_id)},
        )
        return _row_to_attendance(row) if row else None

    async def list_by_session(self, training_session_id: UUID) -> list[Attendance]:
        rows = await self._db.fetch(
            "SELECT * FROM attendances WHERE training_session_id = :session_id",
            {"session_id": str(training_session_id)},
        )
        return [_row_to_attendance(r) for r in rows]

    async def list_by_user(self, user_id: UUID) -> list[Attendance]:
        rows = await self._db.fetch(
            "SELECT * FROM attendances WHERE user_id = :user_id ORDER BY created_at DESC",
            {"user_id": str(user_id)},
        )
        return [_row_to_attendance(r) for r in rows]

    async def count_by_user_in_range(
        self, user_id: UUID, from_dt: datetime, to_dt: datetime
    ) -> int:
        row = await self._db.fetchrow(
            """
            SELECT COUNT(*) AS cnt
            FROM attendances a
            JOIN training_sessions ts ON ts.id = a.training_session_id
            WHERE a.user_id = :user_id AND ts.starts_at >= :from_dt AND ts.starts_at < :to_dt
            """,
            {"user_id": str(user_id), "from_dt": from_dt, "to_dt": to_dt},
        )
        return int(row["cnt"]) if row else 0

    async def count_by_users_in_month(self, year: int, month: int) -> dict[UUID, int]:
        rows = await self._db.fetch_raw(
            """
            SELECT a.user_id, COUNT(*)::int AS cnt
            FROM attendances a
            JOIN training_sessions ts ON ts.id = a.training_session_id
            WHERE EXTRACT(YEAR  FROM ts.starts_at)::int = $1
              AND EXTRACT(MONTH FROM ts.starts_at)::int = $2
            GROUP BY a.user_id
            """,
            year, month,
        )
        return {r["user_id"]: r["cnt"] for r in rows}

    async def count_by_sessions(self, session_ids: list[UUID]) -> dict[UUID, int]:
        if not session_ids:
            return {}
        ids = [str(sid) for sid in session_ids]
        rows = await self._db.fetch_raw(
            """
            SELECT training_session_id, COUNT(*)::int AS cnt
            FROM attendances
            WHERE training_session_id = ANY($1::uuid[])
            GROUP BY training_session_id
            """,
            ids,
        )
        return {r["training_session_id"]: r["cnt"] for r in rows}

    async def count_unique_in_month(self, year: int, month: int) -> int:
        rows = await self._db.fetch_raw(
            """
            SELECT COUNT(DISTINCT a.user_id)::int AS cnt
            FROM attendances a
            JOIN training_sessions ts ON ts.id = a.training_session_id
            WHERE EXTRACT(YEAR  FROM ts.starts_at)::int = $1
              AND EXTRACT(MONTH FROM ts.starts_at)::int = $2
            """,
            year, month,
        )
        return int(rows[0]["cnt"]) if rows else 0

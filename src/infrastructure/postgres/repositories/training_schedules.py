from __future__ import annotations

from uuid import UUID

import asyncpg

from domain.entities.enums.schedule_status import ScheduleStatus
from domain.entities.enums.training_kind import TrainingKind
from domain.entities.enums.weekday import Weekday
from domain.entities.training_schedule import TrainingSchedule
from domain.ports.repositories.training_schedules import TrainingSchedulesRepo
from infrastructure.postgres.db import PostgresDB


def _row_to_schedule(row: asyncpg.Record) -> TrainingSchedule:
    return TrainingSchedule(
        id=row["id"],
        title=row["title"],
        description=row["description"],
        kind=TrainingKind(row["kind"]),
        status=ScheduleStatus(row["status"]),
        weekdays=[Weekday(d) for d in row["weekdays"]],
        time_of_day=row["time_of_day"],
        duration_minutes=row["duration_minutes"],
        location=row["location"],
        instructor_id=row["instructor_id"],
        cost_per_session=row["cost_per_session"],
        populate_from=row["populate_from"],
        populate_to=row["populate_to"],
        created_by=row["created_by"],
        created_at=row["created_at"],
    )


class PostgresTrainingSchedulesRepo(TrainingSchedulesRepo):
    def __init__(self, db: PostgresDB) -> None:
        self._db = db

    async def save(self, schedule: TrainingSchedule) -> None:
        await self._db.execute(
            """
            INSERT INTO training_schedules (
                id, title, description, kind, status, weekdays, time_of_day, duration_minutes,
                location, instructor_id, cost_per_session, populate_from, populate_to,
                created_by, created_at
            ) VALUES (
                :id, :title, :description, :kind, :status, :weekdays, :time_of_day, :duration_minutes,
                :location, :instructor_id, :cost_per_session, :populate_from, :populate_to,
                :created_by, :created_at
            )
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                kind = EXCLUDED.kind,
                status = EXCLUDED.status,
                weekdays = EXCLUDED.weekdays,
                time_of_day = EXCLUDED.time_of_day,
                duration_minutes = EXCLUDED.duration_minutes,
                location = EXCLUDED.location,
                instructor_id = EXCLUDED.instructor_id,
                cost_per_session = EXCLUDED.cost_per_session,
                populate_to = EXCLUDED.populate_to
            """,
            {
                "id": str(schedule.id),
                "title": schedule.title,
                "description": schedule.description,
                "kind": schedule.kind.value,
                "status": schedule.status.value,
                "weekdays": [d.value for d in schedule.weekdays],
                "time_of_day": schedule.time_of_day,
                "duration_minutes": schedule.duration_minutes,
                "location": schedule.location,
                "instructor_id": str(schedule.instructor_id) if schedule.instructor_id else None,
                "cost_per_session": schedule.cost_per_session,
                "populate_from": schedule.populate_from,
                "populate_to": schedule.populate_to,
                "created_by": str(schedule.created_by),
                "created_at": schedule.created_at,
            },
        )

    async def get_by_id(self, schedule_id: UUID) -> TrainingSchedule | None:
        row = await self._db.fetchrow(
            "SELECT * FROM training_schedules WHERE id = :id", {"id": str(schedule_id)}
        )
        return _row_to_schedule(row) if row else None

    async def list_active(self) -> list[TrainingSchedule]:
        rows = await self._db.fetch(
            "SELECT * FROM training_schedules WHERE status = 'active' ORDER BY time_of_day"
        )
        return [_row_to_schedule(r) for r in rows]

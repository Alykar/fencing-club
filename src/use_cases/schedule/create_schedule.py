from __future__ import annotations

import logging
from datetime import UTC, date, datetime, time, timedelta
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.entities.enums.weekday import Weekday
from domain.entities.training_schedule import TrainingSchedule
from domain.entities.training_session import TrainingSession
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.training_schedules import TrainingSchedulesRepo
from domain.ports.repositories.training_sessions import TrainingSessionsRepo

logger = logging.getLogger(__name__)

# Weekday enum → Python weekday int (Mon=0 … Sun=6)
_WD_MAP = {
    Weekday.MONDAY: 0,
    Weekday.TUESDAY: 1,
    Weekday.WEDNESDAY: 2,
    Weekday.THURSDAY: 3,
    Weekday.FRIDAY: 4,
    Weekday.SATURDAY: 5,
    Weekday.SUNDAY: 6,
}


class CreateScheduleInput(BaseModel):
    title: str
    description: str | None = None
    weekdays: list[Weekday]
    time_of_day: time
    duration_minutes: int
    location: str
    populate_from: date
    populate_to: date | None = None
    instructor_id: UUID | None = None
    cost_per_session: float | None = None


class CreateScheduleUseCase:
    def __init__(
        self,
        schedules_repo: TrainingSchedulesRepo,
        sessions_repo: TrainingSessionsRepo,
    ) -> None:
        self._schedules_repo = schedules_repo
        self._sessions_repo = sessions_repo

    @require_role(UserRole.ADMIN)
    async def __call__(
        self, auth_user: AuthUser, data: CreateScheduleInput
    ) -> TrainingSchedule:
        schedule = TrainingSchedule.create(
            title=data.title,
            description=data.description,
            weekdays=data.weekdays,
            time_of_day=data.time_of_day,
            duration_minutes=data.duration_minutes,
            location=data.location,
            populate_from=data.populate_from,
            populate_to=data.populate_to,
            created_by=auth_user.id,
            instructor_id=data.instructor_id,
            cost_per_session=data.cost_per_session,
        )
        await self._schedules_repo.save(schedule)

        # For recurring schedules: generate current month + next month.
        # For one-time events: generate only the single occurrence.
        today = date.today()
        if schedule.weekdays:
            generate_to = _end_of_month(_next_month(today))
        else:
            generate_to = None  # one-time: _generate_sessions handles it

        sessions = generate_sessions(schedule, auth_user.id, to_date=generate_to)
        await self._sessions_repo.save_batch(sessions)

        logger.info(
            "Schedule created schedule_id=%s title=%r weekdays=%s sessions_generated=%d by=%s",
            schedule.id, schedule.title, [d.value for d in schedule.weekdays],
            len(sessions), auth_user.id,
        )
        return schedule


# ─── Public helpers (reused by populate use case) ────────────────────────────

def generate_sessions(
    schedule: TrainingSchedule,
    created_by: UUID,
    *,
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[TrainingSession]:
    """Generate TrainingSession instances for a schedule within the given date window.

    - One-time event (empty weekdays): always exactly one session on populate_from.
    - Recurring: sessions for every matching weekday in [effective_from, effective_to].
      effective_from = max(schedule.populate_from, from_date)
      effective_to   = min(schedule.populate_to,   to_date)  — either may be absent.
    """
    # One-time event: a single session regardless of window
    if not schedule.weekdays:
        starts_at = _combine(schedule.populate_from, schedule.time_of_day)
        return [
            TrainingSession.create(
                title=schedule.title,
                description=schedule.description,
                starts_at=starts_at,
                duration_minutes=schedule.duration_minutes,
                location=schedule.location,
                created_by=created_by,
                schedule_id=schedule.id,
            )
        ]

    effective_from = max(schedule.populate_from, from_date) if from_date else schedule.populate_from

    if schedule.populate_to and to_date:
        effective_to = min(schedule.populate_to, to_date)
    elif schedule.populate_to:
        effective_to = schedule.populate_to
    elif to_date:
        effective_to = to_date
    else:
        # No upper bound at all — should not happen in normal flow
        effective_to = effective_from + timedelta(days=90)

    if effective_from > effective_to:
        return []

    target_weekdays = {_WD_MAP[wd] for wd in schedule.weekdays}
    sessions: list[TrainingSession] = []
    current = effective_from
    while current <= effective_to:
        if current.weekday() in target_weekdays:
            sessions.append(
                TrainingSession.create(
                    title=schedule.title,
                    description=schedule.description,
                    starts_at=_combine(current, schedule.time_of_day),
                    duration_minutes=schedule.duration_minutes,
                    location=schedule.location,
                    created_by=created_by,
                    schedule_id=schedule.id,
                )
            )
        current += timedelta(days=1)
    return sessions


def _combine(d: date, t: time) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute, tzinfo=UTC)


def _next_month(d: date) -> date:
    """First day of the month after d."""
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def _end_of_month(first_of_month: date) -> date:
    """Last day of the month given its first day."""
    next_m = _next_month(first_of_month)
    return next_m - timedelta(days=1)

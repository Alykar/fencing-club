from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from uuid import UUID, uuid4

from domain.entities.enums.schedule_status import ScheduleStatus
from domain.entities.enums.training_kind import TrainingKind
from domain.entities.enums.weekday import Weekday


@dataclass
class TrainingSchedule:
    id: UUID
    title: str
    description: str | None
    kind: TrainingKind
    status: ScheduleStatus
    weekdays: list[Weekday]
    time_of_day: time
    duration_minutes: int
    location: str
    instructor_id: UUID | None
    cost_per_session: float | None
    populate_from: date
    populate_to: date | None  # None = open-ended / one-time events handled by empty weekdays
    created_by: UUID
    created_at: datetime

    @classmethod
    def create(
        cls,
        title: str,
        weekdays: list[Weekday],
        time_of_day: time,
        duration_minutes: int,
        location: str,
        populate_from: date,
        created_by: UUID,
        description: str | None = None,
        populate_to: date | None = None,
        kind: TrainingKind = TrainingKind.OTHER,
        instructor_id: UUID | None = None,
        cost_per_session: float | None = None,
    ) -> TrainingSchedule:
        return cls(
            id=uuid4(),
            title=title,
            description=description,
            kind=kind,
            status=ScheduleStatus.ACTIVE,
            weekdays=weekdays,
            time_of_day=time_of_day,
            duration_minutes=duration_minutes,
            location=location,
            instructor_id=instructor_id,
            cost_per_session=cost_per_session,
            populate_from=populate_from,
            populate_to=populate_to,
            created_by=created_by,
            created_at=datetime.now(UTC),
        )

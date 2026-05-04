from datetime import time, datetime, UTC
from typing import Self
from uuid import UUID, uuid4

from pydantic import BaseModel

from domain.entities.enums.schedule_status import ScheduleStatus
from domain.entities.enums.training_kind import TrainingKind

from domain.entities.enums.weekday import Weekday


class TrainingSchedule(BaseModel):
    id: UUID
    status: ScheduleStatus

    training_session_kind: TrainingKind
    training_session_starts_at: time
    training_session_duration_minutes: int
    training_session_location: str
    training_session_default_instructor_id: UUID | None

    hourly_cost: int
    weekdays_to_populate: list[Weekday]
    populate_from: datetime
    populate_till: datetime | None
    created_by: UUID
    updated_by: UUID
    created_at: datetime

    @classmethod
    def create(
        cls,
        status: ScheduleStatus,
        training_session_kind: TrainingKind,

        training_session_starts_at: time,
        training_session_duration_minutes: int,
        training_session_location: str,
        hourly_cost: int,
        weekdays_to_populate: list[Weekday],
        created_by: UUID,
        training_session_default_instructor_id: UUID | None = None,
        populate_from: datetime | None = None,
        populate_till: datetime | None = None,
    ) -> Self:
        return cls(
            id=uuid4(),
            status=status,
            training_session_kind=training_session_kind,
            training_session_starts_at=training_session_starts_at,
            training_session_duration_minutes=training_session_duration_minutes,
            training_session_location=training_session_location,
            training_session_default_instructor_id=training_session_default_instructor_id,
            hourly_cost=hourly_cost,
            weekdays_to_populate=weekdays_to_populate,
            populate_from=populate_from,
            populate_till=populate_till,
            created_by=created_by,
            updated_by=created_by,
            created_at=datetime.now(UTC),
        )

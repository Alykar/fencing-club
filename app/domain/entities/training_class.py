from datetime import datetime, date, time
from typing import Self
from uuid import UUID, uuid4

from pydantic import BaseModel

from domain.entities.enums.training_class_kind import TrainingClassKind


class TrainingClass(BaseModel):
    id: UUID
    kind: TrainingClassKind
    start_date: date
    starts_at: time
    duration_minutes: int
    location: str
    instructor_id: UUID | None

    @classmethod
    def create(
        cls,
        kind: TrainingClassKind,
        start_date: date,
        starts_at: time,
        duration_minutes: int,
        location: str,
        instructor_id: UUID | None = None,
    ) -> Self:
        return cls(
            id=uuid4(),
            kind=kind,
            starts_at=starts_at,
            start_date=start_date,
            duration_minutes=duration_minutes,
            location=location,
            instructor_id=instructor_id,
        )

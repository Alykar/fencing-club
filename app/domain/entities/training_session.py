from datetime import datetime, date, time, UTC
from typing import Self
from uuid import UUID, uuid4

from pydantic import BaseModel

from domain.entities.enums.training_session_kind import TrainingClassKind


class TrainingSession(BaseModel):
    id: UUID
    kind: TrainingClassKind
    starts_at: datetime
    duration_minutes: int
    location: str
    instructor_id: UUID | None
    created_by: UUID
    updated_by: UUID
    created_at: datetime

    @classmethod
    def create(
        cls,
        kind: TrainingClassKind,
        starts_at: datetime,
        duration_minutes: int,
        location: str,
        created_by: UUID,
        instructor_id: UUID | None = None,
    ) -> Self:
        return cls(
            id=uuid4(),
            kind=kind,
            starts_at=starts_at,
            duration_minutes=duration_minutes,
            location=location,
            instructor_id=instructor_id,
            created_at=datetime.now(UTC),
            created_by=created_by,
            updated_by=created_by,
        )

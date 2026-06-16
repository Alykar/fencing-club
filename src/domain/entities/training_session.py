from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from domain.entities.enums.training_kind import TrainingKind


@dataclass
class TrainingSession:
    id: UUID
    title: str
    description: str | None
    kind: TrainingKind
    starts_at: datetime
    duration_minutes: int
    location: str
    instructor_id: UUID | None
    schedule_id: UUID | None
    created_by: UUID
    updated_by: UUID
    created_at: datetime

    @classmethod
    def create(
        cls,
        title: str,
        starts_at: datetime,
        duration_minutes: int,
        location: str,
        created_by: UUID,
        description: str | None = None,
        kind: TrainingKind = TrainingKind.OTHER,
        instructor_id: UUID | None = None,
        schedule_id: UUID | None = None,
    ) -> TrainingSession:
        now = datetime.now(UTC)
        return cls(
            id=uuid4(),
            title=title,
            description=description,
            kind=kind,
            starts_at=starts_at,
            duration_minutes=duration_minutes,
            location=location,
            instructor_id=instructor_id,
            schedule_id=schedule_id,
            created_by=created_by,
            updated_by=created_by,
            created_at=now,
        )

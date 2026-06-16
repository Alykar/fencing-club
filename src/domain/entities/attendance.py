from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID


@dataclass
class Attendance:
    training_session_id: UUID
    user_id: UUID
    created_by: UUID
    created_at: datetime

    @classmethod
    def create(cls, training_session_id: UUID, user_id: UUID, created_by: UUID) -> Attendance:
        return cls(
            training_session_id=training_session_id,
            user_id=user_id,
            created_by=created_by,
            created_at=datetime.now(UTC),
        )

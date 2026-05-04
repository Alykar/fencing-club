from datetime import datetime, UTC
from typing import Self
from uuid import UUID

from pydantic import BaseModel


class Attendance(BaseModel):
    training_id: UUID
    user_id: UUID
    created_by: UUID
    created_at: datetime

    @classmethod
    def create(cls, training_id: UUID, user_id: UUID, created_by: UUID) -> Self:
        return cls(
            training_id=training_id,
            user_id=user_id,
            created_by=created_by,
            created_at=datetime.now(UTC)
        )

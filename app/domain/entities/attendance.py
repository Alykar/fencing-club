from typing import Self
from uuid import UUID

from pydantic import BaseModel


class Attendance(BaseModel):
    class_id: UUID
    user_id: UUID

    @classmethod
    def create(cls, class_id: UUID, user_id: UUID) -> Self:
        return cls(
            class_id=class_id,
            user_id=user_id,
        )

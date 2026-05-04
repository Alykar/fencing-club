from datetime import date, time, datetime, timedelta, UTC
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, model_validator, Field

from domain.entities.enums.training_kind import TrainingKind
from domain.entities.enums.user_role import UserRole
from domain.errors import LocationIsEmptyError, CannotCreatePastTrainingSessionError, TrainingSessionNotFoundError, \
    TrainingIsPastDueEditWindowError
from domain.interfaces.repos.training_sessions import TrainingSessionsRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class EditTrainingSessionInput(BaseModel):
    training_id: UUID
    kind: TrainingKind
    starts_at: datetime
    duration_minutes: int = Field(ge=0)
    location: str

    @model_validator(mode="after")
    def validate(self, value: Any) -> Self:
        if not self.location.strip():
            raise LocationIsEmptyError

        if self.starts_at < datetime.now(UTC):
            raise CannotCreatePastTrainingSessionError

        return self

class EditTrainingSessionOutput(BaseModel):
    id: UUID
    kind: TrainingKind
    starts_at: datetime
    duration_minutes: int
    location: str
    instructor_id: UUID | None
    created_by: UUID
    updated_by: UUID
    created_at: datetime

class EditTrainingSessionUseCase:
    def __init__(self, trainings: TrainingSessionsRepo) -> None:
        self._trainings = trainings

    @require_auth(UserRole.INSTRUCTOR)
    async def __call__(
        self,
        data: EditTrainingSessionInput,
        auth_user: AuthUser,
    ) -> EditTrainingSessionOutput:
        training_session = await self._trainings.get_by_id(data.training_id)

        if not training_session:
            raise TrainingSessionNotFoundError

        if training_session.starts_at < datetime.now(UTC) + timedelta(days=1):
            raise TrainingIsPastDueEditWindowError

        training_session.kind = data.kind
        training_session.starts_at = data.starts_at
        training_session.duration_minutes = data.duration_minutes
        training_session.location = data.location

        await self._trainings.save(training_session)

        return EditTrainingSessionOutput(
            id=training_session.id,
            kind=training_session.kind,
            starts_at=training_session.starts_at,
            duration_minutes=training_session.duration_minutes,
            location=training_session.location,
            instructor_id=training_session.instructor_id,
            created_by=training_session.created_by,
            updated_by=training_session.updated_by,
            created_at=training_session.created_at,
        )

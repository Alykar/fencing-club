from datetime import date, time, datetime, timedelta, UTC
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, model_validator, Field

from domain.entities.enums.training_kind import TrainingKind
from domain.entities.enums.user_role import UserRole
from domain.entities.training_session import TrainingSession
from domain.errors import LocationIsEmptyError, CannotCreatePastTrainingSessionError, UserNotFoundError
from domain.interfaces.repos.training_sessions import TrainingSessionsRepo
from domain.interfaces.repos.users import UsersRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class CreateTrainingSessionInput(BaseModel):
    kind: TrainingKind
    starts_at: datetime
    duration_minutes: int = Field(ge=0)
    location: str
    instructor_id: UUID | None

    @model_validator(mode="after")
    def validate(self, value: Any) -> Self:
        if not self.location.strip():
            raise LocationIsEmptyError

        if self.starts_at + timedelta(days=30) < datetime.now(UTC):
            raise CannotCreatePastTrainingSessionError

        return self

class CreateTrainingSessionOutput(BaseModel):
    id: UUID
    kind: TrainingKind
    starts_at: datetime
    duration_minutes: int
    location: str
    instructor_id: UUID | None
    created_by: UUID
    updated_by: UUID
    created_at: datetime


class CreateTrainingSessionUseCase:
    def __init__(self, trainings: TrainingSessionsRepo, users: UsersRepo) -> None:
        self._users = users
        self._trainings = trainings

    @require_auth(UserRole.INSTRUCTOR)
    async def __call__(
        self,
        data: CreateTrainingSessionInput,
        auth_user: AuthUser,
    ) -> CreateTrainingSessionOutput:

        if data.instructor_id:
            user = await self._users.get_by_id(data.instructor_id)

            if not user:
                raise UserNotFoundError

        training_session = TrainingSession.create(
            kind=data.kind,
            starts_at=data.starts_at,
            duration_minutes=data.duration_minutes,
            location=data.location,
            instructor_id=data.instructor_id,
            created_by=auth_user.id,
        )
        await self._trainings.save(training_session)

        return CreateTrainingSessionOutput(
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


from datetime import date, time, datetime, timedelta, UTC
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, model_validator

from domain.entities.enums.training_kind import TrainingKind
from domain.entities.enums.user_role import UserRole
from domain.entities.training_session import TrainingSession
from domain.errors import LocationIsEmptyError, CannotCreatePastTrainingSessionError, UserNotFoundError, \
    TrainingSessionNotFoundError, TrainingIsPastDueEditWindowError
from domain.interfaces.repos.training_sessions import TrainingSessionsRepo
from domain.interfaces.repos.users import UsersRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class SetTrainingSessionInstructorInput(BaseModel):
    training_session_id: UUID
    instructor_id: UUID


class SetTrainingSessionInstructorOutput(BaseModel):
    id: UUID
    kind: TrainingKind
    starts_at: datetime
    duration_minutes: int
    location: str
    instructor_id: UUID | None
    created_by: UUID
    updated_by: UUID
    created_at: datetime


class SetTrainingSessionInstructorUseCase:
    def __init__(self, trainings: TrainingSessionsRepo, users: UsersRepo) -> None:
        self._users = users
        self._trainings = trainings

    @require_auth(UserRole.INSTRUCTOR)
    async def __call__(
        self,
        data: SetTrainingSessionInstructorInput,
        auth_user: AuthUser,
    ) -> SetTrainingSessionInstructorOutput:
        user = await self._users.get_by_id(data.instructor_id)

        if not user:
            raise UserNotFoundError

        training_session = await self._trainings.get_by_id(data.training_id)

        if not training_session:
            raise TrainingSessionNotFoundError

        if training_session.starts_at < datetime.now(UTC) + timedelta(days=1):
            raise TrainingIsPastDueEditWindowError

        training_session.instructor_id = data.instructor_id
        await self._trainings.save(training_session)

        return SetTrainingSessionInstructorOutput(
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

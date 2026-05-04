from datetime import datetime, timedelta, UTC
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.errors import TrainingIsPastDueEditWindowError
from domain.interfaces.repos.training_sessions import TrainingSessionsRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class CreateTrainingSessionInput(BaseModel):
    training_session_id: UUID


class CreateTrainingSessionUseCase:
    def __init__(self, trainings: TrainingSessionsRepo) -> None:
        self._trainings = trainings

    @require_auth(UserRole.INSTRUCTOR)
    async def __call__(self, data: CreateTrainingSessionInput, auth_user: AuthUser) -> None:
        training_session = await self._trainings.get_by_id(training_id=data.training_session_id)

        if not training_session:
            return

        if training_session.starts_at < datetime.now(UTC) + timedelta(days=1):
            raise TrainingIsPastDueEditWindowError

        await self._trainings.delete(data.training_session_id)


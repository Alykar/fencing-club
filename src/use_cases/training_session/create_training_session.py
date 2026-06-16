import logging
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.training_kind import TrainingKind
from domain.entities.enums.user_role import UserRole
from domain.entities.training_session import TrainingSession
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.training_sessions import TrainingSessionsRepo

logger = logging.getLogger(__name__)


class CreateTrainingSessionInput(BaseModel):
    title: str
    kind: TrainingKind
    starts_at: datetime
    duration_minutes: int
    location: str
    instructor_id: UUID | None = None
    schedule_id: UUID | None = None


class CreateTrainingSessionUseCase:
    def __init__(self, sessions_repo: TrainingSessionsRepo) -> None:
        self._sessions_repo = sessions_repo

    @require_role(UserRole.ADMIN)
    async def __call__(
        self, auth_user: AuthUser, data: CreateTrainingSessionInput
    ) -> TrainingSession:
        session = TrainingSession.create(
            title=data.title,
            kind=data.kind,
            starts_at=data.starts_at,
            duration_minutes=data.duration_minutes,
            location=data.location,
            created_by=auth_user.id,
            instructor_id=data.instructor_id,
            schedule_id=data.schedule_id,
        )
        await self._sessions_repo.save(session)
        logger.info(
            "Training session created session_id=%s title=%r kind=%s starts_at=%s by=%s",
            session.id, session.title, session.kind, session.starts_at, auth_user.id,
        )
        return session

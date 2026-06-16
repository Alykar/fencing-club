import logging
from uuid import UUID

from domain.entities.attendance import Attendance
from domain.entities.enums.user_role import UserRole
from domain.exceptions import TrainingSessionNotFoundError
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.attendances import AttendancesRepo
from domain.ports.repositories.training_sessions import TrainingSessionsRepo

logger = logging.getLogger(__name__)


class MarkAttendanceUseCase:
    def __init__(
        self,
        attendances_repo: AttendancesRepo,
        sessions_repo: TrainingSessionsRepo,
    ) -> None:
        self._attendances_repo = attendances_repo
        self._sessions_repo = sessions_repo

    @require_role(UserRole.INSTRUCTOR)
    async def __call__(
        self, auth_user: AuthUser, session_id: UUID, user_id: UUID
    ) -> None:
        session = await self._sessions_repo.get_by_id(session_id)
        if not session:
            logger.warning(
                "Mark attendance failed: session not found session_id=%s by=%s",
                session_id, auth_user.id,
            )
            raise TrainingSessionNotFoundError(f"Session {session_id} not found")

        attendance = Attendance.create(
            training_session_id=session_id,
            user_id=user_id,
            created_by=auth_user.id,
        )
        await self._attendances_repo.save(attendance)
        logger.info(
            "Attendance marked session_id=%s user_id=%s by=%s",
            session_id, user_id, auth_user.id,
        )

import logging
from uuid import UUID

from domain.entities.enums.user_role import UserRole
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.attendances import AttendancesRepo

logger = logging.getLogger(__name__)


class UnmarkAttendanceUseCase:
    def __init__(self, attendances_repo: AttendancesRepo) -> None:
        self._attendances_repo = attendances_repo

    @require_role(UserRole.INSTRUCTOR)
    async def __call__(
        self, auth_user: AuthUser, session_id: UUID, user_id: UUID
    ) -> None:
        await self._attendances_repo.delete(session_id, user_id)
        logger.info(
            "Attendance unmarked session_id=%s user_id=%s by=%s",
            session_id, user_id, auth_user.id,
        )

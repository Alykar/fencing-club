import logging
from uuid import UUID

from domain.entities.attendance import Attendance
from domain.entities.enums.user_role import UserRole
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.attendances import AttendancesRepo

logger = logging.getLogger(__name__)


class ListSessionAttendanceUseCase:
    def __init__(self, attendances_repo: AttendancesRepo) -> None:
        self._attendances_repo = attendances_repo

    @require_role(UserRole.INSTRUCTOR)
    async def __call__(self, auth_user: AuthUser, session_id: UUID) -> list[Attendance]:
        logger.debug("Listing attendance session_id=%s by=%s", session_id, auth_user.id)
        return await self._attendances_repo.list_by_session(session_id)

import logging
from datetime import UTC, datetime
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.attendances import AttendancesRepo

logger = logging.getLogger(__name__)


class GetUserStatsResult(BaseModel):
    user_id: UUID
    month: int
    year: int
    sessions_attended: int


class GetUserStatsUseCase:
    def __init__(self, attendances_repo: AttendancesRepo) -> None:
        self._attendances_repo = attendances_repo

    @require_role(UserRole.INSTRUCTOR)
    async def __call__(
        self, auth_user: AuthUser, user_id: UUID, year: int, month: int
    ) -> GetUserStatsResult:
        logger.debug(
            "Fetching stats user_id=%s year=%d month=%d by=%s", user_id, year, month, auth_user.id
        )

        from_dt = datetime(year, month, 1, tzinfo=UTC)
        if month == 12:
            to_dt = datetime(year + 1, 1, 1, tzinfo=UTC)
        else:
            to_dt = datetime(year, month + 1, 1, tzinfo=UTC)

        count = await self._attendances_repo.count_by_user_in_range(user_id, from_dt, to_dt)
        logger.debug(
            "Stats result user_id=%s year=%d month=%d sessions_attended=%d",
            user_id, year, month, count,
        )
        return GetUserStatsResult(
            user_id=user_id,
            month=month,
            year=year,
            sessions_attended=count,
        )

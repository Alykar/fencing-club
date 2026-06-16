import logging
from uuid import UUID

from domain.entities.enums.schedule_status import ScheduleStatus
from domain.entities.enums.user_role import UserRole
from domain.exceptions import TrainingScheduleNotFoundError
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.training_schedules import TrainingSchedulesRepo

logger = logging.getLogger(__name__)


class DeactivateScheduleUseCase:
    def __init__(self, schedules_repo: TrainingSchedulesRepo) -> None:
        self._schedules_repo = schedules_repo

    @require_role(UserRole.ADMIN)
    async def __call__(self, auth_user: AuthUser, schedule_id: UUID) -> None:
        schedule = await self._schedules_repo.get_by_id(schedule_id)
        if not schedule:
            logger.warning(
                "Deactivate schedule failed: not found schedule_id=%s by=%s",
                schedule_id, auth_user.id,
            )
            raise TrainingScheduleNotFoundError(f"Schedule {schedule_id} not found")

        schedule.status = ScheduleStatus.INACTIVE
        await self._schedules_repo.save(schedule)
        logger.info("Schedule deactivated schedule_id=%s by=%s", schedule_id, auth_user.id)

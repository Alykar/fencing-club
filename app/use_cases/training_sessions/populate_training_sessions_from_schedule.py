from datetime import datetime, UTC

from domain.entities.enums.schedule_status import ScheduleStatus
from domain.entities.enums.user_role import UserRole
from domain.interfaces.repos.training_session_schedules import TrainingSchedulesRepo
from domain.interfaces.repos.training_sessions import TrainingSessionsRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class PopulateTrainingSessionsFromScheduleUseCase:
    def __init__(self, trainings: TrainingSessionsRepo, schedules: TrainingSchedulesRepo) -> None:
        self._schedules = schedules
        self._trainings = trainings

    @require_auth(UserRole.INSTRUCTOR)
    async def __call__(self, auth_user: AuthUser) -> None:
        now = datetime.now(UTC)
        schedules = await self._schedules.list_by_status(
            ScheduleStatus.ACTIVE,
            populate_from_lte=now,
            populate_till_gte=now,
        )

        if not schedules:
            return

        # TODO actual creation logic

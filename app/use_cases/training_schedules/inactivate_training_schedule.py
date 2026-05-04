from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.schedule_status import ScheduleStatus
from domain.entities.enums.user_role import UserRole
from domain.errors import TrainingScheduleNotFoundError
from domain.interfaces.repos.training_schedules import TrainingSchedulesRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser

class InactivateTrainingScheduleInput(BaseModel):
    training_schedule_id: UUID


class InactivateTrainingScheduleUseCase:
    def __init__(
        self,
        training_schedules: TrainingSchedulesRepo,
    ) -> None:
        self._training_schedules = training_schedules

    @require_auth(UserRole.ADMIN)
    async def __call__(self, data: InactivateTrainingScheduleInput, auth_user: AuthUser) -> None:
        training_schedule = await self._training_schedules.get_by_id(data.training_schedule_id)

        if not training_schedule:
            raise TrainingScheduleNotFoundError

        training_schedule.status = ScheduleStatus.INACTIVE
        await self._training_schedules.save(training_schedule)

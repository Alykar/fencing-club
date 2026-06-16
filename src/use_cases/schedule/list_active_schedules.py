import logging

from domain.entities.training_schedule import TrainingSchedule
from domain.ports.repositories.training_schedules import TrainingSchedulesRepo

logger = logging.getLogger(__name__)


class ListActiveSchedulesUseCase:
    def __init__(self, schedules_repo: TrainingSchedulesRepo) -> None:
        self._schedules_repo = schedules_repo

    async def __call__(self) -> list[TrainingSchedule]:
        logger.debug("Listing active schedules")
        return await self._schedules_repo.list_active()

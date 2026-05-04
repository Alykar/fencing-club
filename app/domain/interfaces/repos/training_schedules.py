from abc import ABC, abstractmethod
from datetime import datetime
from uuid import UUID

from domain.entities.enums.schedule_status import ScheduleStatus
from domain.entities.training_schedule import TrainingSchedule


class TrainingSchedulesRepo(ABC):

    @abstractmethod
    async def save(self, schedule: TrainingSchedule) -> None:
        ...

    @abstractmethod
    async def list_by_status(
        self,
        status: ScheduleStatus,
        populate_till_gte: datetime,
        populate_from_lte: datetime,
    ) -> list[TrainingSchedule]:
        ...

    @abstractmethod
    async def get_by_id(self, schedule_id: UUID) -> TrainingSchedule | None:
        ...
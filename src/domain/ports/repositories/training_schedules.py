from abc import ABC, abstractmethod
from uuid import UUID

from domain.entities.training_schedule import TrainingSchedule


class TrainingSchedulesRepo(ABC):
    @abstractmethod
    async def save(self, schedule: TrainingSchedule) -> None: ...

    @abstractmethod
    async def get_by_id(self, schedule_id: UUID) -> TrainingSchedule | None: ...

    @abstractmethod
    async def list_active(self) -> list[TrainingSchedule]: ...

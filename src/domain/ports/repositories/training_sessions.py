from abc import ABC, abstractmethod
from datetime import datetime
from uuid import UUID

from domain.entities.training_session import TrainingSession


class TrainingSessionsRepo(ABC):
    @abstractmethod
    async def save(self, session: TrainingSession) -> None: ...

    @abstractmethod
    async def get_by_id(self, session_id: UUID) -> TrainingSession | None: ...

    @abstractmethod
    async def list_by_date_range(
        self, from_dt: datetime, to_dt: datetime
    ) -> list[TrainingSession]: ...

    @abstractmethod
    async def list_upcoming(self, limit: int = 10) -> list[TrainingSession]: ...

    @abstractmethod
    async def save_batch(self, sessions: list[TrainingSession]) -> None: ...

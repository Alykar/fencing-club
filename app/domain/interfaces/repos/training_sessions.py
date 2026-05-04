from abc import ABC, abstractmethod
from uuid import UUID

from domain.entities.training_session import TrainingSession


class TrainingSessionsRepo(ABC):

    @abstractmethod
    async def save(self, training_session: TrainingSession) -> None:
        ...

    @abstractmethod
    async def get_by_id(self, training_id: UUID) -> TrainingSession | None:
        ...

    @abstractmethod
    async def delete(self, training_id: UUID) -> None:
        ...
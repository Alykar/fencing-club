from abc import ABC, abstractmethod
from uuid import UUID

from domain.entities.attendance import Attendance


class AttendancesRepo(ABC):

    @abstractmethod
    async def save(self, attendance: Attendance) -> None:
        ...

    @abstractmethod
    async def get_by_ids(self, user_id: UUID, training_id: UUID) -> Attendance | None:
        ...

    @abstractmethod
    async def delete_by_ids(self, user_id: UUID, training_id: UUID) -> None:
        ...

    @abstractmethod
    async def list_by_user_id(self, user_id: UUID) -> list[Attendance]:
        ...

    @abstractmethod
    async def list_by_training_session_id(self, training_id: UUID) -> list[Attendance]:
        ...

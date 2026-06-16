from abc import ABC, abstractmethod
from datetime import datetime
from uuid import UUID

from domain.entities.attendance import Attendance


class AttendancesRepo(ABC):
    @abstractmethod
    async def save(self, attendance: Attendance) -> None: ...

    @abstractmethod
    async def delete(self, training_session_id: UUID, user_id: UUID) -> None: ...

    @abstractmethod
    async def get(self, training_session_id: UUID, user_id: UUID) -> Attendance | None: ...

    @abstractmethod
    async def list_by_session(self, training_session_id: UUID) -> list[Attendance]: ...

    @abstractmethod
    async def list_by_user(self, user_id: UUID) -> list[Attendance]: ...

    @abstractmethod
    async def count_by_user_in_range(
        self, user_id: UUID, from_dt: datetime, to_dt: datetime
    ) -> int: ...

    @abstractmethod
    async def count_by_users_in_month(
        self, year: int, month: int
    ) -> dict[UUID, int]:
        """Return {user_id: attendance_count} for all users in the given month."""
        ...

    @abstractmethod
    async def count_by_sessions(
        self, session_ids: list[UUID]
    ) -> dict[UUID, int]:
        """Return {session_id: attendance_count} for the given sessions."""
        ...

    @abstractmethod
    async def count_unique_in_month(self, year: int, month: int) -> int:
        """Return count of unique users who attended at least one session in the given month."""
        ...

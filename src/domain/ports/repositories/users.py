from abc import ABC, abstractmethod
from dataclasses import dataclass
from uuid import UUID

from domain.entities.user import User


@dataclass(frozen=True)
class UserSelectItem:
    """Lightweight projection for <select> elements — no password_hash."""
    id: UUID
    name: str


class UsersRepo(ABC):
    @abstractmethod
    async def save(self, user: User) -> None: ...

    @abstractmethod
    async def get_by_id(self, user_id: UUID) -> User | None: ...

    @abstractmethod
    async def get_by_email(self, email: str) -> User | None: ...

    @abstractmethod
    async def list_all(self) -> list[User]: ...

    @abstractmethod
    async def list_pending_access(self) -> list[User]: ...

    @abstractmethod
    async def search(self, query: str) -> list[User]: ...

    @abstractmethod
    async def list_for_select(self) -> list[UserSelectItem]:
        """Return id+name for all access-granted, non-blocked users.

        Used for <select> dropdowns — does not fetch password_hash.
        """
        ...

    @abstractmethod
    async def count_pending(self) -> int:
        """Return count of users with is_access_granted=False and is_blocked=False."""
        ...

from abc import ABC, abstractmethod
from uuid import UUID

from domain.entities.user import User


class UsersRepo(ABC):

    @abstractmethod
    async def save(self, user: User) -> None:
        ...

    @abstractmethod
    async def is_email_taken(self, email: str) -> bool:
        ...

    @abstractmethod
    async def get_by_email(self, email: str) -> User | None:
        ...

    @abstractmethod
    async def get_by_id(self, user_id: UUID) -> User | None:
        ...
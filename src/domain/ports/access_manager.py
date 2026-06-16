from abc import ABC, abstractmethod
from dataclasses import dataclass
from uuid import UUID

from domain.entities.enums.user_role import UserRole


@dataclass
class AuthUser:
    id: UUID
    role: UserRole
    is_blocked: bool
    is_access_granted: bool = True


@dataclass
class KeyPair:
    access_token: str
    refresh_token: str


class AccessManager(ABC):
    @abstractmethod
    def create_tokens(
        self,
        user_id: UUID,
        role: UserRole,
        *,
        is_blocked: bool = False,
        is_access_granted: bool = True,
    ) -> KeyPair: ...

    @abstractmethod
    def decode_access_token(self, token: str) -> AuthUser: ...

    @abstractmethod
    def decode_refresh_token(self, token: str) -> AuthUser: ...

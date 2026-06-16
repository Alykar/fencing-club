from uuid import UUID

from domain.entities.enums.user_role import UserRole
from domain.ports.access_manager import AccessManager, AuthUser, KeyPair


class MockAccessManager(AccessManager):
    def create_tokens(self, user_id: UUID, role: UserRole) -> KeyPair:
        return KeyPair(access_token=f"access:{user_id}", refresh_token=f"refresh:{user_id}")

    def decode_access_token(self, token: str) -> AuthUser:
        user_id = UUID(token.removeprefix("access:"))
        return AuthUser(id=user_id, role=UserRole.USER, is_blocked=False)

    def decode_refresh_token(self, token: str) -> AuthUser:
        user_id = UUID(token.removeprefix("refresh:"))
        return AuthUser(id=user_id, role=UserRole.USER, is_blocked=False)

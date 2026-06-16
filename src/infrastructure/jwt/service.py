from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import jwt

from domain.entities.enums.user_role import UserRole
from domain.ports.access_manager import AccessManager, AuthUser, KeyPair
from infrastructure.jwt.config import JWTConfig


class JWTAccessManager(AccessManager):
    def __init__(self, config: JWTConfig) -> None:
        self._config = config

    def create_tokens(
        self,
        user_id: UUID,
        role: UserRole,
        *,
        is_blocked: bool = False,
        is_access_granted: bool = True,
    ) -> KeyPair:
        access_token = self._encode(
            user_id=user_id,
            role=role,
            ttl_seconds=self._config.access_token_ttl_seconds,
            token_type="access",
            is_blocked=is_blocked,
            is_access_granted=is_access_granted,
        )
        refresh_token = self._encode(
            user_id=user_id,
            role=role,
            ttl_seconds=self._config.refresh_token_ttl_seconds,
            token_type="refresh",
            is_blocked=is_blocked,
            is_access_granted=is_access_granted,
        )
        return KeyPair(access_token=access_token, refresh_token=refresh_token)

    def decode_access_token(self, token: str) -> AuthUser:
        return self._decode(token, expected_type="access")

    def decode_refresh_token(self, token: str) -> AuthUser:
        return self._decode(token, expected_type="refresh")

    def _encode(
        self,
        user_id: UUID,
        role: UserRole,
        ttl_seconds: int,
        token_type: str,
        is_blocked: bool,
        is_access_granted: bool,
    ) -> str:
        now = datetime.now(UTC)
        payload = {
            "sub": str(user_id),
            "role": role.value,
            "type": token_type,
            "blk": is_blocked,
            "acc": is_access_granted,
            "iat": now,
            "exp": now + timedelta(seconds=ttl_seconds),
        }
        secret = self._config.secret.get_secret_value()
        return jwt.encode(payload, secret, algorithm=self._config.algorithm)

    def _decode(self, token: str, expected_type: str) -> AuthUser:
        payload = jwt.decode(
            token, self._config.secret.get_secret_value(), algorithms=[self._config.algorithm]
        )
        if payload.get("type") != expected_type:
            raise ValueError(f"Expected token type {expected_type!r}")
        return AuthUser(
            id=UUID(payload["sub"]),
            role=UserRole(payload["role"]),
            is_blocked=bool(payload.get("blk", False)),
            is_access_granted=bool(payload.get("acc", True)),
        )

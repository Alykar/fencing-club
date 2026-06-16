from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from domain.entities.enums.referral_source import ReferralSource
from domain.entities.enums.user_role import UserRole


@dataclass
class User:
    id: UUID
    name: str
    tel: str
    email: str
    tg: str | None
    vk: str | None
    role: UserRole
    password_hash: str
    is_access_granted: bool  # system login access
    has_hall_pass: bool       # physical gym pass
    is_blocked: bool
    referral_source: ReferralSource
    friend_name: str | None
    created_at: datetime

    @classmethod
    def create(
        cls,
        name: str,
        tel: str,
        email: str,
        password_hash: str,
        referral_source: ReferralSource,
        role: UserRole = UserRole.USER,
        tg: str | None = None,
        vk: str | None = None,
        friend_name: str | None = None,
        is_access_granted: bool = True,
        has_hall_pass: bool = False,
        is_blocked: bool = False,
    ) -> User:
        return cls(
            id=uuid4(),
            name=name,
            tel=tel,
            email=email,
            tg=tg,
            vk=vk,
            role=role,
            password_hash=password_hash,
            is_access_granted=is_access_granted,
            has_hall_pass=has_hall_pass,
            is_blocked=is_blocked,
            referral_source=referral_source,
            friend_name=friend_name,
            created_at=datetime.now(UTC),
        )

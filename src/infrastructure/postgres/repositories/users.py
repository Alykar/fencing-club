from __future__ import annotations

from uuid import UUID

import asyncpg

from domain.entities.enums.referral_source import ReferralSource
from domain.entities.enums.user_role import UserRole
from domain.entities.user import User
from domain.ports.repositories.users import UserSelectItem, UsersRepo
from infrastructure.postgres.db import PostgresDB


def _row_to_user(row: asyncpg.Record) -> User:
    return User(
        id=row["id"],
        name=row["name"],
        tel=row["tel"],
        email=row["email"],
        tg=row["tg"],
        vk=row["vk"],
        role=UserRole(row["role"]),
        password_hash=row["password_hash"],
        is_access_granted=row["is_access_granted"],
        has_hall_pass=row["has_hall_pass"],
        is_blocked=row["is_blocked"],
        referral_source=ReferralSource(row["referral_source"]),
        friend_name=row["friend_name"],
        created_at=row["created_at"],
    )


class PostgresUsersRepo(UsersRepo):
    def __init__(self, db: PostgresDB) -> None:
        self._db = db

    async def save(self, user: User) -> None:
        await self._db.execute(
            """
            INSERT INTO users (
                id, name, tel, email, tg, vk, role, password_hash,
                is_access_granted, has_hall_pass, is_blocked,
                referral_source, friend_name, created_at
            ) VALUES (
                :id, :name, :tel, :email, :tg, :vk, :role, :password_hash,
                :is_access_granted, :has_hall_pass, :is_blocked,
                :referral_source, :friend_name, :created_at
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                tel = EXCLUDED.tel,
                email = EXCLUDED.email,
                tg = EXCLUDED.tg,
                vk = EXCLUDED.vk,
                role = EXCLUDED.role,
                password_hash = EXCLUDED.password_hash,
                is_access_granted = EXCLUDED.is_access_granted,
                has_hall_pass = EXCLUDED.has_hall_pass,
                is_blocked = EXCLUDED.is_blocked,
                referral_source = EXCLUDED.referral_source,
                friend_name = EXCLUDED.friend_name
            """,
            {
                "id": str(user.id),
                "name": user.name,
                "tel": user.tel,
                "email": user.email,
                "tg": user.tg,
                "vk": user.vk,
                "role": user.role.value,
                "password_hash": user.password_hash,
                "is_access_granted": user.is_access_granted,
                "has_hall_pass": user.has_hall_pass,
                "is_blocked": user.is_blocked,
                "referral_source": user.referral_source.value,
                "friend_name": user.friend_name,
                "created_at": user.created_at,
            },
        )

    async def get_by_id(self, user_id: UUID) -> User | None:
        row = await self._db.fetchrow(
            "SELECT * FROM users WHERE id = :id", {"id": str(user_id)}
        )
        return _row_to_user(row) if row else None

    async def get_by_email(self, email: str) -> User | None:
        row = await self._db.fetchrow(
            "SELECT * FROM users WHERE email = :email", {"email": email}
        )
        return _row_to_user(row) if row else None

    async def list_all(self) -> list[User]:
        rows = await self._db.fetch("SELECT * FROM users ORDER BY name")
        return [_row_to_user(r) for r in rows]

    async def list_pending_access(self) -> list[User]:
        rows = await self._db.fetch(
            "SELECT * FROM users WHERE has_hall_pass = FALSE AND is_blocked = FALSE ORDER BY created_at ASC"
        )
        return [_row_to_user(r) for r in rows]

    async def search(self, query: str) -> list[User]:
        pattern = f"%{query}%"
        rows = await self._db.fetch(
            """
            SELECT * FROM users
            WHERE name ILIKE :pattern OR email ILIKE :pattern OR tel ILIKE :pattern
            ORDER BY name
            """,
            {"pattern": pattern},
        )
        return [_row_to_user(r) for r in rows]

    async def list_for_select(self) -> list[UserSelectItem]:
        rows = await self._db.fetch(
            """
            SELECT id, name FROM users
            WHERE is_access_granted = TRUE AND is_blocked = FALSE
            ORDER BY name
            """
        )
        return [UserSelectItem(id=r["id"], name=r["name"]) for r in rows]

    async def count_pending(self) -> int:
        row = await self._db.fetchrow(
            "SELECT COUNT(*) AS cnt FROM users WHERE has_hall_pass = FALSE AND is_blocked = FALSE"
        )
        return int(row["cnt"]) if row else 0

from __future__ import annotations

from uuid import UUID

import asyncpg

from domain.entities.weapon_type import WeaponType
from domain.ports.repositories.weapon_types import WeaponTypesRepo
from infrastructure.postgres.db import PostgresDB


def _row_to_weapon_type(row: asyncpg.Record) -> WeaponType:
    return WeaponType(
        id=row["id"],
        name=row["name"],
        is_default=row["is_default"],
    )


class PostgresWeaponTypesRepo(WeaponTypesRepo):
    def __init__(self, db: PostgresDB) -> None:
        self._db = db

    async def save(self, weapon_type: WeaponType) -> None:
        await self._db.execute(
            """
            INSERT INTO weapon_types (id, name, is_default)
            VALUES (:id, :name, :is_default)
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
            """,
            {
                "id": str(weapon_type.id),
                "name": weapon_type.name,
                "is_default": weapon_type.is_default,
            },
        )

    async def get_by_id(self, weapon_type_id: UUID) -> WeaponType | None:
        row = await self._db.fetchrow(
            "SELECT * FROM weapon_types WHERE id = :id", {"id": str(weapon_type_id)}
        )
        return _row_to_weapon_type(row) if row else None

    async def list_all(self) -> list[WeaponType]:
        rows = await self._db.fetch("SELECT * FROM weapon_types ORDER BY name")
        return [_row_to_weapon_type(r) for r in rows]

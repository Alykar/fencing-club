from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4


@dataclass
class WeaponType:
    id: UUID
    name: str
    is_default: bool

    @classmethod
    def create(cls, name: str, is_default: bool = False) -> WeaponType:
        return cls(id=uuid4(), name=name, is_default=is_default)

from abc import ABC, abstractmethod
from uuid import UUID

from domain.entities.weapon_type import WeaponType


class WeaponTypesRepo(ABC):
    @abstractmethod
    async def save(self, weapon_type: WeaponType) -> None: ...

    @abstractmethod
    async def get_by_id(self, weapon_type_id: UUID) -> WeaponType | None: ...

    @abstractmethod
    async def list_all(self) -> list[WeaponType]: ...

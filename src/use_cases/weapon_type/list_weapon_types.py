import logging

from domain.entities.weapon_type import WeaponType
from domain.ports.repositories.weapon_types import WeaponTypesRepo

logger = logging.getLogger(__name__)


class ListWeaponTypesUseCase:
    def __init__(self, weapon_types_repo: WeaponTypesRepo) -> None:
        self._weapon_types_repo = weapon_types_repo

    async def __call__(self) -> list[WeaponType]:
        logger.debug("Listing weapon types")
        return await self._weapon_types_repo.list_all()

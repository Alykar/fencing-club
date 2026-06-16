import logging

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.entities.weapon_type import WeaponType
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.weapon_types import WeaponTypesRepo

logger = logging.getLogger(__name__)


class CreateWeaponTypeInput(BaseModel):
    name: str


class CreateWeaponTypeUseCase:
    def __init__(self, weapon_types_repo: WeaponTypesRepo) -> None:
        self._weapon_types_repo = weapon_types_repo

    @require_role(UserRole.INSTRUCTOR)
    async def __call__(self, auth_user: AuthUser, data: CreateWeaponTypeInput) -> WeaponType:
        weapon_type = WeaponType.create(name=data.name)
        await self._weapon_types_repo.save(weapon_type)
        logger.info(
            "Weapon type created weapon_type_id=%s name=%r by=%s",
            weapon_type.id, weapon_type.name, auth_user.id,
        )
        return weapon_type

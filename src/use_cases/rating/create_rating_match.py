import logging
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.entities.rating_match import RatingMatch
from domain.exceptions import UserNotFoundError, WeaponTypeNotFoundError
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.rating_matches import RatingMatchesRepo
from domain.ports.repositories.users import UsersRepo
from domain.ports.repositories.weapon_types import WeaponTypesRepo
from domain.ports.unit_of_work import UnitOfWork

logger = logging.getLogger(__name__)


class CreateRatingMatchInput(BaseModel):
    left_user_id: UUID
    right_user_id: UUID
    weapon_type_ids: list[UUID]


class CreateRatingMatchUseCase:
    def __init__(
        self,
        rating_matches_repo: RatingMatchesRepo,
        users_repo: UsersRepo,
        weapon_types_repo: WeaponTypesRepo,
        uow: UnitOfWork,
    ) -> None:
        self._rating_matches_repo = rating_matches_repo
        self._users_repo = users_repo
        self._weapon_types_repo = weapon_types_repo
        self._uow = uow

    @require_role(UserRole.INSTRUCTOR)
    async def __call__(
        self, auth_user: AuthUser, data: CreateRatingMatchInput
    ) -> RatingMatch:
        logger.debug(
            "Creating rating match left=%s right=%s weapons=%s by=%s",
            data.left_user_id, data.right_user_id, data.weapon_type_ids, auth_user.id,
        )

        for user_id in (data.left_user_id, data.right_user_id):
            if not await self._users_repo.get_by_id(user_id):
                logger.warning(
                    "Create rating match failed: user not found user_id=%s by=%s",
                    user_id, auth_user.id,
                )
                raise UserNotFoundError(f"User {user_id} not found")

        for wt_id in data.weapon_type_ids:
            if not await self._weapon_types_repo.get_by_id(wt_id):
                logger.warning(
                    "Create rating match failed: weapon type not found weapon_type_id=%s by=%s",
                    wt_id, auth_user.id,
                )
                raise WeaponTypeNotFoundError(f"Weapon type {wt_id} not found")

        async with self._uow.transaction():
            match = RatingMatch.create(
                left_user_id=data.left_user_id,
                right_user_id=data.right_user_id,
                weapon_type_ids=data.weapon_type_ids,
                created_by=auth_user.id,
            )
            await self._rating_matches_repo.save_match(match)

            bout = match.start_next_bout()
            await self._rating_matches_repo.save_bout(bout)

        logger.info(
            "Rating match created match_id=%s left=%s right=%s by=%s",
            match.id, data.left_user_id, data.right_user_id, auth_user.id,
        )
        return match

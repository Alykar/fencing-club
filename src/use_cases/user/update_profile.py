import logging
from uuid import UUID

from pydantic import BaseModel

from domain.exceptions import ForbiddenError, UserNotFoundError
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.users import UsersRepo

logger = logging.getLogger(__name__)


class UpdateProfileInput(BaseModel):
    name: str | None = None
    tel: str | None = None
    tg: str | None = None
    vk: str | None = None


class UpdateProfileUseCase:
    def __init__(self, users_repo: UsersRepo) -> None:
        self._users_repo = users_repo

    async def __call__(self, auth_user: AuthUser, user_id: UUID, data: UpdateProfileInput) -> None:
        if auth_user.id != user_id:
            logger.warning(
                "Update profile forbidden: caller=%s target=%s", auth_user.id, user_id
            )
            raise ForbiddenError(f"User {auth_user.id} cannot edit profile of {user_id}")

        logger.debug("Updating profile user_id=%s fields=%s", user_id, data.model_fields_set)
        user = await self._users_repo.get_by_id(user_id)
        if not user:
            logger.warning("Update profile failed: user not found user_id=%s", user_id)
            raise UserNotFoundError(f"User {user_id} not found")

        if data.name is not None:
            user.name = data.name
        if data.tel is not None:
            user.tel = data.tel
        if data.tg is not None:
            user.tg = data.tg
        if data.vk is not None:
            user.vk = data.vk

        await self._users_repo.save(user)
        logger.info(
            "Profile updated user_id=%s fields=%s by=%s",
            user_id, data.model_fields_set, auth_user.id,
        )

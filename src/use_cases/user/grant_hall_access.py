import logging
from uuid import UUID

from domain.entities.enums.user_role import UserRole
from domain.exceptions import UserNotFoundError
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.users import UsersRepo

logger = logging.getLogger(__name__)


class GrantHallAccessUseCase:
    def __init__(self, users_repo: UsersRepo) -> None:
        self._users_repo = users_repo

    @require_role(UserRole.INSTRUCTOR)
    async def __call__(self, auth_user: AuthUser, user_id: UUID) -> None:
        user = await self._users_repo.get_by_id(user_id)
        if not user:
            logger.warning(
                "Grant hall access failed: user not found user_id=%s by=%s",
                user_id, auth_user.id,
            )
            raise UserNotFoundError(f"User {user_id} not found")

        user.has_hall_pass = True
        await self._users_repo.save(user)
        logger.info("Hall pass granted user_id=%s by=%s", user_id, auth_user.id)

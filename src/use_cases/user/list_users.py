import logging

from domain.entities.enums.user_role import UserRole
from domain.entities.user import User
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.users import UsersRepo

logger = logging.getLogger(__name__)


class ListUsersUseCase:
    def __init__(self, users_repo: UsersRepo) -> None:
        self._users_repo = users_repo

    @require_role(UserRole.INSTRUCTOR)
    async def __call__(self, auth_user: AuthUser, query: str | None = None) -> list[User]:
        if query:
            logger.debug("Searching users query=%r by=%s", query, auth_user.id)
            return await self._users_repo.search(query)
        logger.debug("Listing all users by=%s", auth_user.id)
        return await self._users_repo.list_all()

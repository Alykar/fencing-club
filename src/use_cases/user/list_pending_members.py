import logging

from domain.entities.enums.user_role import UserRole
from domain.entities.user import User
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.users import UsersRepo

logger = logging.getLogger(__name__)


class ListPendingMembersUseCase:
    def __init__(self, users_repo: UsersRepo) -> None:
        self._users_repo = users_repo

    @require_role(UserRole.INSTRUCTOR)
    async def __call__(self, auth_user: AuthUser) -> list[User]:
        logger.debug("Listing pending members by=%s", auth_user.id)
        return await self._users_repo.list_pending_access()

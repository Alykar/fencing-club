import logging
from uuid import UUID

from domain.entities.user import User
from domain.exceptions import UserNotFoundError
from domain.ports.repositories.users import UsersRepo

logger = logging.getLogger(__name__)


class GetProfileUseCase:
    def __init__(self, users_repo: UsersRepo) -> None:
        self._users_repo = users_repo

    async def __call__(self, user_id: UUID) -> User:
        logger.debug("Fetching profile user_id=%s", user_id)
        user = await self._users_repo.get_by_id(user_id)
        if not user:
            logger.warning("Get profile failed: user not found user_id=%s", user_id)
            raise UserNotFoundError(f"User {user_id} not found")
        return user

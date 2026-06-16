import logging

from domain.entities.enums.user_role import UserRole
from domain.entities.rating_match import UserRating
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.rating_matches import UserRatingsRepo

logger = logging.getLogger(__name__)


class GetLeaderboardUseCase:
    def __init__(self, user_ratings_repo: UserRatingsRepo) -> None:
        self._user_ratings_repo = user_ratings_repo

    @require_role(UserRole.USER)
    async def __call__(self, auth_user: AuthUser, limit: int = 50) -> list[UserRating]:
        logger.debug("Fetching leaderboard limit=%d by=%s", limit, auth_user.id)
        return await self._user_ratings_repo.list_leaderboard(limit=limit)

import logging
from uuid import UUID

from domain.entities.enums.user_role import UserRole
from domain.entities.match import Match
from domain.exceptions import MatchNotFoundError
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.matches import MatchesRepo

logger = logging.getLogger(__name__)


class EndMatchUseCase:
    def __init__(self, matches_repo: MatchesRepo) -> None:
        self._matches_repo = matches_repo

    @require_role(UserRole.USER)
    async def __call__(self, auth_user: AuthUser, match_id: UUID) -> Match:
        match = await self._matches_repo.get_by_id(match_id)
        if not match:
            logger.warning("End match failed: not found match_id=%s by=%s", match_id, auth_user.id)
            raise MatchNotFoundError(f"Match {match_id} not found")

        match.complete()
        await self._matches_repo.save(match)
        logger.info(
            "Match ended match_id=%s winner=%s score=%d:%d by=%s",
            match.id, match.winner, match.left_score, match.right_score, auth_user.id,
        )
        return match

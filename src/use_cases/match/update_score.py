import logging
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.entities.match import Match, MatchSide
from domain.exceptions import MatchNotFoundError
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.matches import MatchesRepo

logger = logging.getLogger(__name__)


class UpdateScoreInput(BaseModel):
    side: MatchSide
    delta: int


class UpdateScoreUseCase:
    def __init__(self, matches_repo: MatchesRepo) -> None:
        self._matches_repo = matches_repo

    @require_role(UserRole.USER)
    async def __call__(
        self, auth_user: AuthUser, match_id: UUID, data: UpdateScoreInput
    ) -> Match:
        match = await self._matches_repo.get_by_id(match_id)
        if not match:
            logger.warning(
                "Update score failed: match not found match_id=%s by=%s", match_id, auth_user.id
            )
            raise MatchNotFoundError(f"Match {match_id} not found")

        for _ in range(abs(data.delta)):
            if data.delta > 0:
                match.add_point(data.side)
            else:
                match.remove_point(data.side)

        await self._matches_repo.save(match)
        logger.debug(
            "Score updated match_id=%s side=%s delta=%+d score=%d:%d by=%s",
            match_id, data.side, data.delta, match.left_score, match.right_score, auth_user.id,
        )
        return match

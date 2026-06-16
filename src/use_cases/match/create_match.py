import logging

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.entities.match import Match
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.matches import MatchesRepo

logger = logging.getLogger(__name__)


class CreateMatchInput(BaseModel):
    title: str
    left_label: str
    right_label: str
    max_score: int | None = None
    duration_seconds: int | None = None


class CreateMatchUseCase:
    def __init__(self, matches_repo: MatchesRepo) -> None:
        self._matches_repo = matches_repo

    @require_role(UserRole.USER)
    async def __call__(self, auth_user: AuthUser, data: CreateMatchInput) -> Match:
        match = Match.create(
            title=data.title,
            left_label=data.left_label,
            right_label=data.right_label,
            created_by=auth_user.id,
            max_score=data.max_score,
            duration_seconds=data.duration_seconds,
        )
        await self._matches_repo.save(match)
        logger.info(
            "Match created match_id=%s title=%r left=%r right=%r by=%s",
            match.id, match.title, match.left_label, match.right_label, auth_user.id,
        )
        return match

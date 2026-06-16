import logging
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.entities.rating_match import RatingMatch, UserRating
from domain.exceptions import RatingMatchNotFoundError
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.rating_matches import RatingMatchesRepo, UserRatingsRepo
from domain.ports.unit_of_work import UnitOfWork

logger = logging.getLogger(__name__)


class RecordPassInput(BaseModel):
    winner_user_id: UUID


class RecordPassUseCase:
    def __init__(
        self,
        rating_matches_repo: RatingMatchesRepo,
        user_ratings_repo: UserRatingsRepo,
        uow: UnitOfWork,
    ) -> None:
        self._rating_matches_repo = rating_matches_repo
        self._user_ratings_repo = user_ratings_repo
        self._uow = uow

    @require_role(UserRole.INSTRUCTOR)
    async def __call__(
        self, auth_user: AuthUser, match_id: UUID, data: RecordPassInput
    ) -> RatingMatch:
        logger.debug(
            "Recording pass match_id=%s winner=%s by=%s",
            match_id, data.winner_user_id, auth_user.id,
        )

        async with self._uow.transaction():
            match = await self._rating_matches_repo.get_match_by_id(match_id)
            if not match:
                logger.warning(
                    "Record pass failed: match not found match_id=%s by=%s",
                    match_id, auth_user.id,
                )
                raise RatingMatchNotFoundError(f"Rating match {match_id} not found")

            bout = match.get_current_bout()
            if bout is None:
                bout = match.start_next_bout()
                await self._rating_matches_repo.save_bout(bout)

            rating_pass = bout.record_pass(data.winner_user_id)
            await self._rating_matches_repo.save_pass(rating_pass)
            logger.debug(
                "Pass saved match_id=%s bout=%d pass=%d winner=%s",
                match_id, bout.bout_number, rating_pass.pass_number, data.winner_user_id,
            )

            if bout.is_complete:
                await self._rating_matches_repo.save_bout(bout)
                logger.debug("Bout completed match_id=%s bout=%d", match_id, bout.bout_number)

                if match.try_complete():
                    await self._rating_matches_repo.save_match(match)
                    logger.info(
                        "Rating match completed match_id=%s winner=%s",
                        match_id, match.winner_user_id,
                    )
                    await self._update_elo(match)
                else:
                    next_bout = match.start_next_bout()
                    await self._rating_matches_repo.save_bout(next_bout)
                    logger.debug(
                        "Next bout started match_id=%s bout=%d", match_id, next_bout.bout_number
                    )

        return match

    async def _update_elo(self, match: RatingMatch) -> None:
        left_rating = await self._user_ratings_repo.get_by_user_id(match.left_user_id)
        right_rating = await self._user_ratings_repo.get_by_user_id(match.right_user_id)

        if not left_rating:
            left_rating = UserRating.create_default(match.left_user_id)
        if not right_rating:
            right_rating = UserRating.create_default(match.right_user_id)

        # Снимаем ELO до обновления — оба используют pre-match значения (исправление бага)
        left_elo_before = left_rating.elo
        right_elo_before = right_rating.elo

        left_score, right_score = match.calculate_scores()
        left_won = match.winner_user_id == match.left_user_id

        left_rating.update_elo(right_elo_before, left_score, won=left_won)
        right_rating.update_elo(left_elo_before, right_score, won=not left_won)

        # Снимки ELO для отображения на экране результата
        match.left_elo_before = left_elo_before
        match.right_elo_before = right_elo_before
        await self._rating_matches_repo.save_match(match)

        await self._user_ratings_repo.save(left_rating)
        await self._user_ratings_repo.save(right_rating)

        logger.info(
            "ELO updated match_id=%s "
            "left=%s elo=%.1f→%.1f (convincing=%.2f won=%s) "
            "right=%s elo=%.1f→%.1f (convincing=%.2f won=%s)",
            match.id,
            match.left_user_id, left_elo_before, left_rating.elo, left_score, left_won,
            match.right_user_id, right_elo_before, right_rating.elo, right_score, not left_won,
        )

import logging
from datetime import UTC, datetime, timedelta

from domain.entities.training_session import TrainingSession
from domain.ports.repositories.training_sessions import TrainingSessionsRepo

logger = logging.getLogger(__name__)


class ListUpcomingSessionsUseCase:
    def __init__(self, sessions_repo: TrainingSessionsRepo) -> None:
        self._sessions_repo = sessions_repo

    async def __call__(self, weeks_ahead: int = 1) -> list[TrainingSession]:
        now = datetime.now(UTC)
        to_dt = now + timedelta(weeks=weeks_ahead)
        logger.debug("Listing upcoming sessions from=%s to=%s", now.date(), to_dt.date())
        return await self._sessions_repo.list_by_date_range(now, to_dt)

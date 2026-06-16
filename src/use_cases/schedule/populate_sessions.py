from __future__ import annotations

import logging
from datetime import date

from domain.ports.repositories.training_schedules import TrainingSchedulesRepo
from domain.ports.repositories.training_sessions import TrainingSessionsRepo
from use_cases.schedule.create_schedule import (
    _end_of_month,
    _next_month,
    generate_sessions,
)

logger = logging.getLogger(__name__)


class PopulateSessionsForMonthUseCase:
    """Generate training sessions for all active recurring schedules for a given month.

    Idempotent: ON CONFLICT DO NOTHING means calling it twice is safe.
    created_by is set to the schedule's own created_by (the admin who created it).
    """

    def __init__(
        self,
        schedules_repo: TrainingSchedulesRepo,
        sessions_repo: TrainingSessionsRepo,
    ) -> None:
        self._schedules_repo = schedules_repo
        self._sessions_repo = sessions_repo

    async def __call__(self, year: int, month: int) -> int:
        """Returns count of sessions inserted (skipped ones are not counted)."""
        from_date = date(year, month, 1)
        to_date = _end_of_month(from_date)

        schedules = await self._schedules_repo.list_active()
        total = 0

        for schedule in schedules:
            if not schedule.weekdays:
                continue  # one-time events are not auto-extended
            if schedule.populate_to and schedule.populate_to < from_date:
                continue  # schedule ended before this month

            sessions = generate_sessions(
                schedule,
                schedule.created_by,  # реальный пользователь, создавший расписание
                from_date=from_date,
                to_date=to_date,
            )
            if sessions:
                await self._sessions_repo.save_batch(sessions)
                total += len(sessions)
                logger.debug(
                    "Populated %d sessions for schedule_id=%s month=%d-%02d",
                    len(sessions), schedule.id, year, month,
                )

        logger.info(
            "populate_sessions month=%d-%02d schedules=%d sessions_generated=%d",
            year, month, len(schedules), total,
        )
        return total


def target_month_for_populate(today: date | None = None) -> tuple[int, int]:
    """Return (year, month) that should be populated on the 1st of the current month.

    Logic: always stay 2 months ahead.
      1 Jun → populate Aug
      1 Jul → populate Sep
    """
    d = today or date.today()
    two_ahead = _next_month(_next_month(d))
    return two_ahead.year, two_ahead.month

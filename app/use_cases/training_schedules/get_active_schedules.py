from datetime import datetime, UTC, time
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.schedule_status import ScheduleStatus
from domain.entities.enums.training_kind import TrainingKind
from domain.entities.enums.weekday import Weekday
from domain.interfaces.repos.training_schedules import TrainingSchedulesRepo


class ScheduleListItem(BaseModel):
    id: UUID
    training_session_kind: TrainingKind
    training_session_starts_at: time
    training_session_duration_minutes: int
    training_session_location: str
    weekdays_to_populate: list[Weekday]


class GetActiveSchedulesOutput(BaseModel):
    schedule: list[ScheduleListItem]


class GetActiveSchedulesUseCase:
    def __init__(self, schedules: TrainingSchedulesRepo) -> None:
        self._schedules = schedules


    async def __call__(self) -> GetActiveSchedulesOutput:
        now = datetime.now(UTC)
        schedules = await self._schedules.list_by_status(
            ScheduleStatus.ACTIVE,
            populate_from_lte=now,
            populate_till_gte=now,
        )

        return GetActiveSchedulesOutput(
            schedule=[
                ScheduleListItem(
                    id=training_schedule.id,
                    training_session_kind=training_schedule.training_session_kind,
                    training_session_starts_at=training_schedule.training_session_starts_at,
                    training_session_duration_minutes=training_schedule.training_session_duration_minutes,
                    training_session_location=training_schedule.training_session_location,
                    weekdays_to_populate=training_schedule.weekdays_to_populate,
                ) for training_schedule in schedules
            ]
        )
from datetime import time, datetime, UTC
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, model_validator, Field

from domain.entities.enums.schedule_status import ScheduleStatus
from domain.entities.enums.training_kind import TrainingKind
from domain.entities.enums.user_role import UserRole
from domain.entities.enums.weekday import Weekday
from domain.entities.training_schedule import TrainingSchedule
from domain.errors import UserNotFoundError, CannotCreatePastTrainingSessionError, InvalidDatesError, \
    ScheduleWeekdaysMissingError, LocationIsEmptyError
from domain.interfaces.repos.training_schedules import TrainingSchedulesRepo
from domain.interfaces.repos.users import UsersRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class CreateTrainingScheduleInput(BaseModel):
    status: ScheduleStatus
    training_session_kind: TrainingKind
    training_session_starts_at: time
    training_session_duration_minutes: int = Field(ge=0)
    training_session_location: str
    training_session_default_instructor_id: UUID | None

    hourly_cost: int = Field(ge=0)
    weekdays_to_populate: list[Weekday]
    populate_from: datetime
    populate_till: datetime | None

    @model_validator(mode="after")
    def validate(self, value: Any) -> Self:
        if self.populate_from > self.populate_till:
            raise InvalidDatesError

        if self.populate_from < datetime.now(UTC):
            raise CannotCreatePastTrainingSessionError

        if self.populate_till < datetime.now(UTC):
            raise CannotCreatePastTrainingSessionError

        if not self.training_session_location.strip():
            raise LocationIsEmptyError

        if not self.weekdays_to_populate:
            raise ScheduleWeekdaysMissingError

        return self

class CreateTrainingScheduleOutput(BaseModel):
    id: UUID
    status: ScheduleStatus

    training_session_kind: TrainingKind
    training_session_starts_at: time
    training_session_duration_minutes: int
    training_session_location: str
    training_session_default_instructor_id: UUID | None

    hourly_cost: int
    weekdays_to_populate: list[Weekday]
    populate_from: datetime
    populate_till: datetime | None
    created_by: UUID
    updated_by: UUID
    created_at: datetime

class CreateTrainingScheduleUseCase:
    def __init__(
        self,
        training_schedules: TrainingSchedulesRepo,
        users: UsersRepo,
    ) -> None:
        self._training_schedules = training_schedules
        self._users = users

    @require_auth(UserRole.ADMIN)
    async def __call__(self, data: CreateTrainingScheduleInput, auth_user: AuthUser) -> CreateTrainingScheduleOutput:
        if data.training_session_default_instructor_id:
            user = await self._users.get_by_id(data.training_session_default_instructor_id)

            if not user or user.role not in [UserRole.ADMIN, UserRole.ADMIN]:
                raise UserNotFoundError

        training_schedule = TrainingSchedule.create(
            status=data.status,
            training_session_kind=data.training_session_kind,

            training_session_starts_at=data.training_session_starts_at,
            training_session_duration_minutes=data.training_session_duration_minutes,
            training_session_location=data.training_session_location,
            hourly_cost=data.hourly_cost,
            weekdays_to_populate=data.weekdays_to_populate,
            created_by=auth_user.id,
            training_session_default_instructor_id=data.training_session_default_instructor_id,
            populate_from=data.populate_from,
            populate_till=data.populate_till,
        )
        await self._training_schedules.save(training_schedule)

        return CreateTrainingScheduleOutput(
            id=training_schedule.id,
            status=training_schedule.status,
            training_session_kind=training_schedule.training_session_kind,
            training_session_starts_at=training_schedule.training_session_starts_at,
            training_session_duration_minutes=training_schedule.training_session_duration_minutes,
            training_session_location=training_schedule.training_session_location,
            training_session_default_instructor_id=training_schedule.training_session_default_instructor_id,
            hourly_cost=training_schedule.hourly_cost,
            weekdays_to_populate=training_schedule.weekdays_to_populate,
            populate_from=training_schedule.populate_from,
            populate_till=training_schedule.populate_till,
            created_by=training_schedule.created_by,
            updated_by=training_schedule.updated_by,
            created_at=training_schedule.created_at,
        )

from uuid import UUID

from pydantic import BaseModel

from domain.entities.attendance import Attendance
from domain.entities.enums.user_role import UserRole
from domain.errors import UserNotFoundError, TrainingSessionNotFoundError
from domain.interfaces.repos.attendances import AttendancesRepo
from domain.interfaces.repos.training_sessions import TrainingSessionsRepo
from domain.interfaces.repos.users import UsersRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class CreateAttendanceInput(BaseModel):
    user_id: UUID
    training_session_id: UUID



class CreateAttendanceUseCase:
    def __init__(self, users: UsersRepo, trainings: TrainingSessionsRepo, attendances: AttendancesRepo) -> None:
        self._attendances = attendances
        self._trainings = trainings
        self._users = users

    @require_auth(UserRole.INSTRUCTOR)
    async def __call__(self, data: CreateAttendanceInput, auth_user: AuthUser) -> None:
        user = await self._users.get_by_id(data.user_id)

        if not user:
            raise UserNotFoundError

        training = await self._trainings.get_by_id(data.training_session_id)

        if not training:
            raise TrainingSessionNotFoundError

        attendance = Attendance.create(
            training_id=training.id,
            user_id=user.id,
            created_by=auth_user.id,
        )
        await self._attendances.save(attendance)

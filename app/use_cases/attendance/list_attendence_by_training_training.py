from uuid import UUID

from pydantic import BaseModel

from domain.entities.attendance import Attendance
from domain.entities.enums.user_role import UserRole
from domain.interfaces.repos.attendances import AttendancesRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class ListAttendanceByTrainingSessionInput(BaseModel):
    training_session_id: UUID


class ListAttendanceByTrainingSessionOutput(BaseModel):
    attendances: list[Attendance]


class ListAttendanceByTrainingSessionUseCase:
    def __init__(self, attendances: AttendancesRepo) -> None:
        self._attendances = attendances

    @require_auth(UserRole.INSTRUCTOR)
    async def __call__(
        self,
        data: ListAttendanceByTrainingSessionInput,
        auth_user: AuthUser,
    ) -> ListAttendanceByTrainingSessionOutput:
        attendances = await self._attendances.list_by_training_session_id(data.training_session_id)

        return ListAttendanceByTrainingSessionOutput(attendances=attendances)

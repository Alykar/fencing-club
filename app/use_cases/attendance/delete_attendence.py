from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.interfaces.repos.attendances import AttendancesRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class DeleteAttendanceInput(BaseModel):
    user_id: UUID
    training_session_id: UUID



class DeleteAttendanceUseCase:
    def __init__(self, attendances: AttendancesRepo) -> None:
        self._attendances = attendances

    @require_auth(UserRole.INSTRUCTOR)
    async def __call__(self, data: DeleteAttendanceInput, auth_user: AuthUser) -> None:
        attendance = await self._attendances.get_by_ids(
            user_id=data.user_id,
            training_id=data.training_session_id,
        )

        if not attendance:
            return

        await self._attendances.delete_by_ids(
            user_id=data.user_id,
            training_id=data.training_session_id,
        )

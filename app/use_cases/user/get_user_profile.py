from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.errors import UserNotFoundError
from domain.interfaces.repos.users import UsersRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class GetUserProfileOutput(BaseModel):
    id: UUID
    tg: str | None
    vk: str | None
    tel: str
    email: str
    name: str
    role: UserRole
    is_access_granted: bool


class GetUserProfileUseCase:
    def __init__(self, users: UsersRepo) -> None:
        self._users = users

    @require_auth(UserRole.USER)
    async def __call__(self, auth_user: AuthUser) -> GetUserProfileOutput:
        user = await self._users.get_by_id(user_id=auth_user.id)

        if not user:
            raise UserNotFoundError

        return GetUserProfileOutput(
            id=user.id,
            name=user.name,
            tg=user.tg,
            vk=user.vk,
            tel=user.tel,
            email=user.email,
            role=user.role,
            is_access_granted=user.is_access_granted,
        )
from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.errors import UserNotFoundError
from domain.interfaces.repos.users import UsersRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class UpdateUserInput(BaseModel):
    tg: str | None = None
    vk: str | None = None
    tel: str | None = None
    email: str | None = None
    name: str | None = None


class UpdateUserUseCase:
    def __init__(self, users: UsersRepo) -> None:
        self._users = users

    @require_auth(UserRole.USER)
    async def __call__(self, data: UpdateUserInput, auth_user: AuthUser) -> None:
        user = await self._users.get_by_id(user_id=data.user_id)

        if not user or (user.id != auth_user.id and auth_user.role == UserRole.USER):
            raise UserNotFoundError

        user.tg = data.tg if data.tg else user.tg
        user.vk = data.vk if data.vk else user.vk
        user.tel = data.tel if data.tel else user.tel
        user.name = data.name if data.name else user.name

        await self._users.save(user)
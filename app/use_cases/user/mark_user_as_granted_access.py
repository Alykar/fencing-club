from uuid import UUID

from pydantic import BaseModel

from domain.errors import UserNotFoundError
from domain.interfaces.repos.users import UsersRepo
from domain.utills.classes.auth_user import AuthUser


class MarkUserAsGrantedAccessInput(BaseModel):
    user_id: UUID


class MarkUserAsGrantedAccessUseCase:
    def __init__(self, users: UsersRepo) -> None:
        self._users = users

    async def __call__(self, data: MarkUserAsGrantedAccessInput, auth_user: AuthUser) -> None:
        user = await self._users.get_by_id(user_id=data.user_id)

        if not user:
            raise UserNotFoundError

        user.is_access_granted = True

        await self._users.save(user)

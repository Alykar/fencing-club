from uuid import UUID

from pydantic import BaseModel

from domain.errors import UserNotFoundError, PasswordMismatchError
from domain.interfaces.access_manager import AccessManager
from domain.interfaces.password_manager import PasswordManager
from domain.interfaces.repos.users import UsersRepo


class LoginInput(BaseModel):
    email: str
    password: str

class LoginOutput(BaseModel):
    id: UUID
    name: str
    access_key: str
    refresh_key: str

class LogInUseCase:
    def __init__(
        self, 
        users: UsersRepo, 
        access_manager: AccessManager,
        password_manager: PasswordManager,
    ) -> None:
        self._password_manager = password_manager
        self._access_manager = access_manager
        self._users = users

    async def __call__(self, data: LoginInput) -> LoginOutput:
        user = await self._users.get_by_email(email=data.email)

        if not user:
            raise UserNotFoundError

        try:
            self._password_manager.verify_password(password=data.password, reference=user.password_hash)
        except PasswordMismatchError as e:
            raise UserNotFoundError from e

        key_pair = await self._access_manager.grant_access(user)

        return LoginOutput(
            id=user.id,
            name=user.name,
            access_key=key_pair.access,
            refresh_key=key_pair.refresh,
        )
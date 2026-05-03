from typing import Self

from pydantic import BaseModel, model_validator

from domain.entities.enums.user_role import UserRole
from domain.entities.user import User
from domain.errors import AdditionalContactRequiredError, PasswordsDoNotMatchError, UserAlreadyExistsError
from domain.interfaces.password_manager import PasswordManager
from domain.interfaces.repos.users import UsersRepo


class RegisterInput(BaseModel):
    first_name: str
    middle_name: str | None
    last_name: str
    email: str
    password: str
    password2: str
    tg: str | None
    vk: str | None
    tel: str | None

    @model_validator(mode="after")
    def validate(self) -> Self:
        if not (self.tg or self.vk or self.tel):
            raise AdditionalContactRequiredError

        if self.password != self.password2:
            raise PasswordsDoNotMatchError

        return self

class RegisterUseCase:
    def __init__(self, users: UsersRepo, password_manager: PasswordManager) -> None:
        self._password_manager = password_manager
        self._users = users

    async def __call__(self, data: RegisterInput) -> None:
        if await self._users.is_email_taken(data.email):
            raise UserAlreadyExistsError

        user = User.create(
            name=f"{data.last_name} {data.first_name} {data.middle_name}",
            email=data.email,
            role=UserRole.USER,
            tg=data.tg,
            vk=data.vk,
            tel=data.tel,
            password_hash=self._password_manager.hash_password(data.password)
        )

        await self._users.save(user)


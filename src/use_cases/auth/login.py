import logging

from pydantic import BaseModel, field_validator

from domain.exceptions import PasswordMismatchError, UserBlockedError, UserNotFoundError
from domain.ports.access_manager import AccessManager, KeyPair
from domain.ports.password_manager import PasswordManager
from domain.ports.repositories.users import UsersRepo

logger = logging.getLogger(__name__)


class LoginInput(BaseModel):
    email: str
    password: str

    @field_validator("password")
    @classmethod
    def password_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Пароль не может быть пустым")
        return v


class LoginUseCase:
    def __init__(
        self,
        users_repo: UsersRepo,
        password_manager: PasswordManager,
        access_manager: AccessManager,
    ) -> None:
        self._users_repo = users_repo
        self._password_manager = password_manager
        self._access_manager = access_manager

    async def __call__(self, data: LoginInput) -> KeyPair:
        logger.debug("Login attempt for email=%s", data.email)

        user = await self._users_repo.get_by_email(data.email)
        if not user:
            logger.warning("Login failed: user not found email=%s", data.email)
            raise UserNotFoundError(f"User {data.email!r} not found")

        if user.is_blocked:
            logger.warning("Login failed: user blocked user_id=%s email=%s", user.id, data.email)
            raise UserBlockedError(f"User {user.id} is blocked")

        if not self._password_manager.verify(data.password, user.password_hash):
            logger.warning("Login failed: wrong password user_id=%s email=%s", user.id, data.email)
            raise PasswordMismatchError("Invalid password")

        logger.info("User logged in user_id=%s email=%s role=%s", user.id, data.email, user.role)
        return self._access_manager.create_tokens(
            user.id,
            user.role,
            is_blocked=user.is_blocked,
            is_access_granted=user.is_access_granted,
        )

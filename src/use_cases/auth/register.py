import logging
import re

from pydantic import BaseModel, field_validator

from domain.entities.enums.referral_source import ReferralSource
from domain.entities.user import User
from domain.exceptions import UserAlreadyExistsError
from domain.ports.password_manager import PasswordManager
from domain.ports.repositories.users import UsersRepo

logger = logging.getLogger(__name__)

_PASSWORD_MIN_LENGTH = 8
_PHONE_RE = re.compile(r"^\+?[0-9\s\-\(\)]{7,20}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class RegisterInput(BaseModel):
    last_name: str
    first_name: str
    patronymic: str | None = None
    tel: str
    email: str
    password: str
    password_confirm: str
    referral_source: ReferralSource
    friend_name: str | None = None
    tg: str | None = None
    vk: str | None = None

    @property
    def full_name(self) -> str:
        parts = [self.last_name, self.first_name]
        if self.patronymic:
            parts.append(self.patronymic)
        return " ".join(parts)

    @field_validator("last_name", "first_name")
    @classmethod
    def name_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Поле обязательно для заполнения")
        return v

    @field_validator("email")
    @classmethod
    def email_valid(cls, v: str) -> str:
        v = v.strip().lower()
        if not _EMAIL_RE.match(v):
            raise ValueError("Введите корректный email")
        return v

    @field_validator("tel")
    @classmethod
    def tel_valid(cls, v: str) -> str:
        v = v.strip()
        if not _PHONE_RE.match(v):
            raise ValueError("Введите корректный номер телефона")
        return v

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        if len(v) < _PASSWORD_MIN_LENGTH:
            raise ValueError(f"Пароль должен содержать минимум {_PASSWORD_MIN_LENGTH} символов")
        return v


class RegisterUseCase:
    def __init__(
        self,
        users_repo: UsersRepo,
        password_manager: PasswordManager,
    ) -> None:
        self._users_repo = users_repo
        self._password_manager = password_manager

    async def __call__(self, data: RegisterInput) -> None:
        """Register a new user. Does NOT issue tokens — access is pending admin approval."""
        logger.debug("Registration attempt email=%s name=%s", data.email, data.full_name)

        if data.password != data.password_confirm:
            from domain.exceptions import PasswordsDoNotMatchError
            logger.warning("Registration failed: passwords mismatch email=%s", data.email)
            raise PasswordsDoNotMatchError("Passwords do not match")

        if await self._users_repo.get_by_email(data.email):
            logger.warning("Registration failed: email already taken email=%s", data.email)
            raise UserAlreadyExistsError(f"Email {data.email!r} already taken")

        user = User.create(
            name=data.full_name,
            tel=data.tel,
            email=data.email,
            password_hash=self._password_manager.hash(data.password),
            referral_source=data.referral_source,
            tg=data.tg,
            vk=data.vk,
            friend_name=data.friend_name,
        )
        await self._users_repo.save(user)
        logger.info(
            "New user registered user_id=%s email=%s name=%s referral=%s",
            user.id, data.email, data.full_name, data.referral_source,
        )

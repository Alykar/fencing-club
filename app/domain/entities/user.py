from typing import Self
from uuid import UUID, uuid4

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole


class User(BaseModel):
    id: UUID
    tg: str | None
    vk: str | None
    tel: str
    email: str
    name: str
    role: UserRole
    password_hash: str
    is_access_granted: bool

    @classmethod
    def create(
        cls,
        name: str,
        tel: str,
        email: str,
        password_hash: str,
        role: UserRole = UserRole.USER,
        tg: str | None = None,
        vk: str | None = None,
        is_access_granted: bool = False
    ) -> Self:
        return cls(
            id=uuid4(),
            name=name,
            tel=tel,
            email=email,
            password_hash=password_hash,
            role=role,
            tg=tg,
            vk=vk,
            is_access_granted=is_access_granted,
        )

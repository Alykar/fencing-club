from uuid import UUID

from domain.entities.enums.user_role import UserRole


class AuthUser:
    id: UUID
    role: UserRole

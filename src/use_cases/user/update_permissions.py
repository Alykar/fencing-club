import logging
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.exceptions import UserNotFoundError
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.users import UsersRepo

logger = logging.getLogger(__name__)


class UpdatePermissionsInput(BaseModel):
    role: UserRole | None = None
    is_blocked: bool | None = None


class UpdatePermissionsUseCase:
    def __init__(self, users_repo: UsersRepo) -> None:
        self._users_repo = users_repo

    @require_role(UserRole.ADMIN)
    async def __call__(
        self, auth_user: AuthUser, user_id: UUID, data: UpdatePermissionsInput
    ) -> None:
        user = await self._users_repo.get_by_id(user_id)
        if not user:
            logger.warning(
                "Update permissions failed: user not found user_id=%s by=%s",
                user_id, auth_user.id,
            )
            raise UserNotFoundError(f"User {user_id} not found")

        changes: dict[str, object] = {}
        if data.role is not None:
            changes["role"] = f"{user.role}→{data.role}"
            user.role = data.role
        if data.is_blocked is not None:
            changes["is_blocked"] = f"{user.is_blocked}→{data.is_blocked}"
            user.is_blocked = data.is_blocked

        await self._users_repo.save(user)
        logger.info(
            "Permissions updated user_id=%s changes=%s by=%s", user_id, changes, auth_user.id
        )

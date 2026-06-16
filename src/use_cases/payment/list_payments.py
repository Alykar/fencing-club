import logging
from uuid import UUID

from domain.entities.enums.user_role import UserRole
from domain.entities.payment import Payment
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.payments import PaymentsRepo

logger = logging.getLogger(__name__)


class ListPaymentsUseCase:
    def __init__(self, payments_repo: PaymentsRepo) -> None:
        self._payments_repo = payments_repo

    @require_role(UserRole.INSTRUCTOR)
    async def __call__(self, auth_user: AuthUser, user_id: UUID) -> list[Payment]:
        logger.debug("Listing payments user_id=%s by=%s", user_id, auth_user.id)
        return await self._payments_repo.list_by_user(user_id)

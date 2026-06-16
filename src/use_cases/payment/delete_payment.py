import logging
from uuid import UUID

from domain.entities.enums.user_role import UserRole
from domain.exceptions import PaymentNotFoundError
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.payments import PaymentsRepo

logger = logging.getLogger(__name__)


class DeletePaymentUseCase:
    def __init__(self, payments_repo: PaymentsRepo) -> None:
        self._payments_repo = payments_repo

    @require_role(UserRole.ADMIN)
    async def __call__(self, auth_user: AuthUser, payment_id: UUID) -> None:
        if not await self._payments_repo.get_by_id(payment_id):
            logger.warning(
                "Delete payment failed: not found payment_id=%s by=%s", payment_id, auth_user.id
            )
            raise PaymentNotFoundError(f"Payment {payment_id} not found")

        await self._payments_repo.delete(payment_id)
        logger.info("Payment deleted payment_id=%s by=%s", payment_id, auth_user.id)

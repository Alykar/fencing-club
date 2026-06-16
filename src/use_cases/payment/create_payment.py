import logging
from datetime import date
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.entities.payment import Payment
from domain.exceptions import UserNotFoundError
from domain.permission_check import require_role
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.payments import PaymentsRepo
from domain.ports.repositories.users import UsersRepo

logger = logging.getLogger(__name__)


class CreatePaymentInput(BaseModel):
    user_id: UUID
    amount: float
    paid_at: date
    is_one_time: bool = False
    note: str | None = None


class CreatePaymentUseCase:
    def __init__(self, payments_repo: PaymentsRepo, users_repo: UsersRepo) -> None:
        self._payments_repo = payments_repo
        self._users_repo = users_repo

    @require_role(UserRole.ADMIN)
    async def __call__(self, auth_user: AuthUser, data: CreatePaymentInput) -> Payment:
        if not await self._users_repo.get_by_id(data.user_id):
            logger.warning(
                "Create payment failed: user not found user_id=%s by=%s",
                data.user_id, auth_user.id,
            )
            raise UserNotFoundError(f"User {data.user_id} not found")

        payment = Payment.create(
            user_id=data.user_id,
            amount=data.amount,
            paid_at=data.paid_at,
            created_by=auth_user.id,
            is_one_time=data.is_one_time,
            note=data.note,
        )
        await self._payments_repo.save(payment)
        logger.info(
            "Payment created payment_id=%s user_id=%s amount=%.2f paid_at=%s one_time=%s by=%s",
            payment.id, data.user_id, data.amount, data.paid_at, data.is_one_time, auth_user.id,
        )
        return payment

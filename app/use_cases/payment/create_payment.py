from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.entities.payment import Payment
from domain.errors import UserNotFoundError
from domain.interfaces.repos.payments import PaymentsRepo
from domain.interfaces.repos.users import UsersRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class CreatePaymentInput(BaseModel):
    amount: int
    payer_id: UUID
    payed_at: date
    is_one_time: bool


class CreatePaymentUseCase:
    def __init__(self, users: UsersRepo, payments: PaymentsRepo) -> None:
        self._payments = payments
        self._users = users

    @require_auth(UserRole.ADMIN)
    async def __call__(self, data: CreatePaymentInput, auth_user: AuthUser) -> None:
        payer = await self._users.get_by_id(data.payer_id)

        if not payer:
            raise UserNotFoundError

        payment = Payment.create(
            amount=data.amount,
            payer_id=data.payer_id,
            payed_at=data.payed_at,
            is_one_time=data.is_one_time,
            created_by=auth_user.id,
        )
        await self._payments.save(payment)

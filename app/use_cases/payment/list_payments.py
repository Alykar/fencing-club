from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel

from domain.entities.enums.user_role import UserRole
from domain.interfaces.repos.payments import PaymentsRepo
from domain.permission_check import require_auth
from domain.utills.classes.auth_user import AuthUser


class ListPaymentsInput(BaseModel):
    user_id: UUID | None = None
    include_one_time: bool = True
    date_gte: date | None = None
    date_lte: date | None = None


class PaymentListItem(BaseModel):
    id: UUID
    payer_id: UUID
    amount: int
    payed_at: date
    is_one_time: bool
    created_by: UUID
    processed_at: datetime


class ListPaymentsOutput(BaseModel):
    payments: list[PaymentListItem]

class ListPaymentsUseCase:
    def __init__(self, payments: PaymentsRepo):
        self._payments = payments

    @require_auth(UserRole.INSTRUCTOR)
    async def __call__(self, data, auth_user: AuthUser) -> ListPaymentsOutput:
        payments = await self._payments.filter_payments()

        return ListPaymentsOutput(
            payments=[
                PaymentListItem(
                    id=payment.id,
                    payer_id=payment.payer_id,
                    amount=payment.amount,
                    payed_at=payment.payed_at,
                    is_one_time=payment.is_one_time,
                    created_by=payment.created_by,
                    processed_at=payment.processed_at,
                ) for payment in payments
            ]
        )
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID

from domain.entities.payment import Payment


@dataclass(frozen=True)
class UserPaymentSummary:
    user_id: UUID
    total_amount: float
    payments_count: int
    last_paid_at: date | None
    last_processed_at: datetime | None


@dataclass(frozen=True)
class MonthlyPaymentRow:
    """Per-user data for a selected month in the payment table."""
    user_id: UUID
    last_regular_paid_at: date | None    # last non-one-time payment (ever)
    last_regular_amount: float | None    # amount of that payment
    month_amount: float                  # total paid in selected month


class PaymentsRepo(ABC):
    @abstractmethod
    async def save(self, payment: Payment) -> None: ...

    @abstractmethod
    async def delete(self, payment_id: UUID) -> None: ...

    @abstractmethod
    async def get_by_id(self, payment_id: UUID) -> Payment | None: ...

    @abstractmethod
    async def list_by_user(self, user_id: UUID) -> list[Payment]: ...

    @abstractmethod
    async def list_paid_user_ids(self, user_ids: list[UUID]) -> set[UUID]:
        """Return subset of user_ids that have at least one payment record."""
        ...

    @abstractmethod
    async def list_active_subscriber_ids(
        self, user_ids: list[UUID], as_of: date
    ) -> set[UUID]:
        """Return subset of user_ids whose subscription is active as of the given date.

        Active = last non-one-time payment paid_at + 1 month >= as_of.
        """
        ...

    @abstractmethod
    async def summary_all_users(self) -> list[UserPaymentSummary]:
        """Return payment summary per user (for overview table)."""
        ...

    @abstractmethod
    async def monthly_summary(self, year: int, month: int) -> list[MonthlyPaymentRow]:
        """Per-user summary for a specific month."""
        ...

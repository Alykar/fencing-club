from abc import ABC, abstractmethod
from datetime import date
from uuid import UUID

from domain.entities.payment import Payment


class PaymentsRepo(ABC):

    @abstractmethod
    async def save(self, payment: Payment) -> None:
        ...

    @abstractmethod
    async def filter_payments(
        self,
        payer_id: UUID | None = None,
        include_one_time: bool = True,
        date_gte: date | None = None,
        date_lte: date | None = None,
    ) -> list[Payment]:
        ...

from datetime import datetime, date
from typing import Self
from uuid import UUID, uuid4

from pydantic import BaseModel

from domain.entities.enums.payment_kind import PaymentKind


class Payment(BaseModel):
    id: UUID
    amount: int
    payed_at: date
    is_one_time: bool
    processed_at: datetime

    @classmethod
    def create(
        cls,
        amount: int,
        payed_at: date,
        is_one_time: bool,
        processed_at: datetime
    ) -> Self:
        return cls(
            id=uuid4(),
            amount=amount,
            payed_at=payed_at,
            is_one_time=is_one_time,
            processed_at=processed_at
        )
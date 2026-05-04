from datetime import datetime, date, UTC
from typing import Self
from uuid import UUID, uuid4

from pydantic import BaseModel


class Payment(BaseModel):
    id: UUID
    amount: int
    payer_id: UUID
    payed_at: date
    is_one_time: bool
    processed_at: datetime
    created_by: UUID

    @classmethod
    def create(
        cls,
        amount: int,
        payer_id: UUID,
        payed_at: date,
        is_one_time: bool,
        created_by: UUID,
    ) -> Self:
        return cls(
            id=uuid4(),
            payer_id=payer_id,
            amount=amount,
            payed_at=payed_at,
            is_one_time=is_one_time,
            created_by=created_by,
            processed_at=datetime.now(UTC),
        )
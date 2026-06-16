from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from uuid import UUID, uuid4


@dataclass
class Payment:
    id: UUID
    user_id: UUID
    amount: float
    paid_at: date
    processed_at: datetime
    is_one_time: bool
    note: str | None
    created_by: UUID
    created_at: datetime

    @classmethod
    def create(
        cls,
        user_id: UUID,
        amount: float,
        paid_at: date,
        created_by: UUID,
        is_one_time: bool = False,
        note: str | None = None,
    ) -> Payment:
        now = datetime.now(UTC)
        return cls(
            id=uuid4(),
            user_id=user_id,
            amount=amount,
            paid_at=paid_at,
            processed_at=now,
            is_one_time=is_one_time,
            note=note,
            created_by=created_by,
            created_at=now,
        )

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4

from domain.entities.enums.match_status import MatchStatus


class MatchSide(StrEnum):
    LEFT = "left"
    RIGHT = "right"


@dataclass
class Match:
    id: UUID
    title: str
    left_label: str
    right_label: str
    max_score: int | None
    duration_seconds: int | None
    status: MatchStatus
    left_score: int
    right_score: int
    winner: MatchSide | None
    created_by: UUID
    created_at: datetime

    @classmethod
    def create(
        cls,
        title: str,
        left_label: str,
        right_label: str,
        created_by: UUID,
        max_score: int | None = None,
        duration_seconds: int | None = None,
    ) -> Match:
        return cls(
            id=uuid4(),
            title=title,
            left_label=left_label,
            right_label=right_label,
            max_score=max_score,
            duration_seconds=duration_seconds,
            status=MatchStatus.ONGOING,
            left_score=0,
            right_score=0,
            winner=None,
            created_by=created_by,
            created_at=datetime.now(UTC),
        )

    def add_point(self, side: MatchSide) -> None:
        if self.status != MatchStatus.ONGOING:
            return
        if side == MatchSide.LEFT:
            self.left_score += 1
        else:
            self.right_score += 1
        if self.max_score and max(self.left_score, self.right_score) >= self.max_score:
            self._finalize()

    def remove_point(self, side: MatchSide) -> None:
        if self.status != MatchStatus.ONGOING:
            return
        if side == MatchSide.LEFT:
            self.left_score = max(0, self.left_score - 1)
        else:
            self.right_score = max(0, self.right_score - 1)

    def complete(self) -> None:
        self._finalize()

    def cancel(self) -> None:
        self.status = MatchStatus.CANCELLED

    def _finalize(self) -> None:
        self.status = MatchStatus.COMPLETED
        if self.left_score > self.right_score:
            self.winner = MatchSide.LEFT
        elif self.right_score > self.left_score:
            self.winner = MatchSide.RIGHT

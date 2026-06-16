from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

from domain.entities.enums.match_status import MatchStatus

BOUTS_PER_MATCH = 3
PASSES_PER_BOUT = 3

POINTS_PER_PASS = 0.1
POINTS_PER_BOUT = 0.1
POINTS_PER_MATCH = 0.1

# Maximum convincing score (all exchanges + all fights + match): 9×0.1 + 3×0.1 + 0.1 = 1.3
MAX_CONVINCING_SCORE = 1.3

ELO_DEFAULT = 1000.0

# Glicko-inspired K factor: K = BASE_K × (RD / 100)
# RD = MAX_RD × exp(−matches / 10) + MIN_RD
GLICKO_BASE_K = 30.0
GLICKO_MIN_RD = 50.0
GLICKO_MAX_RD = 350.0


@dataclass
class RatingPass:
    id: UUID
    bout_id: UUID
    pass_number: int
    winner_user_id: UUID

    @classmethod
    def create(cls, bout_id: UUID, pass_number: int, winner_user_id: UUID) -> RatingPass:
        return cls(
            id=uuid4(), bout_id=bout_id, pass_number=pass_number, winner_user_id=winner_user_id
        )


@dataclass
class RatingBout:
    id: UUID
    match_id: UUID
    bout_number: int
    winner_user_id: UUID | None
    passes: list[RatingPass] = field(default_factory=list)

    @classmethod
    def create(cls, match_id: UUID, bout_number: int) -> RatingBout:
        return cls(id=uuid4(), match_id=match_id, bout_number=bout_number, winner_user_id=None)

    def record_pass(self, winner_user_id: UUID) -> RatingPass:
        """Record one pass. Bout completes after exactly PASSES_PER_BOUT passes.
        Winner = player who won the majority (e.g. 2 out of 3)."""
        rating_pass = RatingPass.create(
            bout_id=self.id,
            pass_number=len(self.passes) + 1,
            winner_user_id=winner_user_id,
        )
        self.passes.append(rating_pass)

        if len(self.passes) >= PASSES_PER_BOUT:
            wins_per_user: dict[UUID, int] = {}
            for p in self.passes:
                wins_per_user[p.winner_user_id] = wins_per_user.get(p.winner_user_id, 0) + 1
            self.winner_user_id = max(wins_per_user, key=lambda k: wins_per_user[k])

        return rating_pass

    @property
    def is_complete(self) -> bool:
        return len(self.passes) >= PASSES_PER_BOUT


@dataclass
class RatingMatch:
    id: UUID
    left_user_id: UUID
    right_user_id: UUID
    weapon_type_ids: list[UUID]
    status: MatchStatus
    winner_user_id: UUID | None
    bouts: list[RatingBout] = field(default_factory=list)
    created_by: UUID = field(default_factory=uuid4)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    left_elo_before: float | None = None
    right_elo_before: float | None = None

    @classmethod
    def create(
        cls,
        left_user_id: UUID,
        right_user_id: UUID,
        weapon_type_ids: list[UUID],
        created_by: UUID,
    ) -> RatingMatch:
        return cls(
            id=uuid4(),
            left_user_id=left_user_id,
            right_user_id=right_user_id,
            weapon_type_ids=weapon_type_ids,
            status=MatchStatus.ONGOING,
            winner_user_id=None,
            bouts=[],
            created_by=created_by,
            created_at=datetime.now(UTC),
        )

    def start_next_bout(self) -> RatingBout:
        bout = RatingBout.create(match_id=self.id, bout_number=len(self.bouts) + 1)
        self.bouts.append(bout)
        return bout

    def get_current_bout(self) -> RatingBout | None:
        for bout in reversed(self.bouts):
            if not bout.is_complete:
                return bout
        return None

    def calculate_scores(self) -> tuple[float, float]:
        """Возвращает (left_points, right_points)."""
        left, right = 0.0, 0.0

        for bout in self.bouts:
            for p in bout.passes:
                if p.winner_user_id == self.left_user_id:
                    left += POINTS_PER_PASS
                else:
                    right += POINTS_PER_PASS
            if bout.winner_user_id == self.left_user_id:
                left += POINTS_PER_BOUT
            elif bout.winner_user_id == self.right_user_id:
                right += POINTS_PER_BOUT

        if self.winner_user_id == self.left_user_id:
            left += POINTS_PER_MATCH
        elif self.winner_user_id == self.right_user_id:
            right += POINTS_PER_MATCH

        return left, right

    def try_complete(self) -> bool:
        """Завершает матч только когда все BOUTS_PER_MATCH боёв сыграны.
        Победитель — тот, кто выиграл больше боёв (2:1 или 3:0)."""
        completed = [b for b in self.bouts if b.is_complete]
        if len(completed) < BOUTS_PER_MATCH:
            return False

        left_wins = sum(1 for b in completed if b.winner_user_id == self.left_user_id)
        right_wins = sum(1 for b in completed if b.winner_user_id == self.right_user_id)

        self.winner_user_id = (
            self.left_user_id if left_wins >= right_wins else self.right_user_id
        )
        self.status = MatchStatus.COMPLETED
        return True


@dataclass
class UserRating:
    user_id: UUID
    elo: float
    matches_played: int

    @classmethod
    def create_default(cls, user_id: UUID) -> UserRating:
        return cls(user_id=user_id, elo=ELO_DEFAULT, matches_played=0)

    @property
    def is_calibrated(self) -> bool:
        return self.matches_played >= 10

    def _rd(self) -> float:
        """Rating Deviation: высокий у новичков (350), падает к 50 с опытом."""
        rd = GLICKO_MAX_RD * math.exp(-self.matches_played / 10.0) + GLICKO_MIN_RD
        return min(GLICKO_MAX_RD, max(GLICKO_MIN_RD, rd))

    def update_elo(self, opponent_elo_before: float, convincing_score: float, won: bool) -> None:
        """Обновляет рейтинг по системе убедительных очков (Glicko-адаптация).

        convincing_score — очки от 0.0 до MAX_CONVINCING_SCORE (1.3):
            +0.1 за каждый выигранный сход  (макс 0.9)
            +0.1 за каждый выигранный бой   (макс 0.3)
            +0.1 за победу в матче          (0 или 0.1)

        won — победил ли боец в матче (защита: победитель не теряет очки).
        opponent_elo_before — ELO соперника ДО обновления.
        """
        rd = self._rd()
        k = GLICKO_BASE_K * (rd / 100.0)

        rating_diff = self.elo - opponent_elo_before
        win_prob = 1.0 / (1.0 + 10.0 ** (-rating_diff / 400.0))
        expected_norm = win_prob  # ожидаемый normalized score [0..1]
        actual_norm = convincing_score / MAX_CONVINCING_SCORE

        change = k * (actual_norm - expected_norm)

        # Победитель не теряет очки
        if won and change < 0:
            change = 0.0

        self.elo = round(self.elo + change, 2)
        self.matches_played += 1

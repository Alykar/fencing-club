from __future__ import annotations

from uuid import UUID

import asyncpg

from domain.entities.enums.match_status import MatchStatus
from domain.entities.match import Match, MatchSide
from domain.ports.repositories.matches import MatchesRepo
from infrastructure.postgres.db import PostgresDB


def _row_to_match(row: asyncpg.Record) -> Match:
    return Match(
        id=row["id"],
        title=row["title"],
        left_label=row["left_label"],
        right_label=row["right_label"],
        max_score=row["max_score"],
        duration_seconds=row["duration_seconds"],
        status=MatchStatus(row["status"]),
        left_score=row["left_score"],
        right_score=row["right_score"],
        winner=MatchSide(row["winner"]) if row["winner"] else None,
        created_by=row["created_by"],
        created_at=row["created_at"],
    )


class PostgresMatchesRepo(MatchesRepo):
    def __init__(self, db: PostgresDB) -> None:
        self._db = db

    async def save(self, match: Match) -> None:
        await self._db.execute(
            """
            INSERT INTO matches (
                id, title, left_label, right_label, max_score, duration_seconds,
                status, left_score, right_score, winner, created_by, created_at
            ) VALUES (
                :id, :title, :left_label, :right_label, :max_score, :duration_seconds,
                :status, :left_score, :right_score, :winner, :created_by, :created_at
            )
            ON CONFLICT (id) DO UPDATE SET
                status = EXCLUDED.status,
                left_score = EXCLUDED.left_score,
                right_score = EXCLUDED.right_score,
                winner = EXCLUDED.winner
            """,
            {
                "id": str(match.id),
                "title": match.title,
                "left_label": match.left_label,
                "right_label": match.right_label,
                "max_score": match.max_score,
                "duration_seconds": match.duration_seconds,
                "status": match.status.value,
                "left_score": match.left_score,
                "right_score": match.right_score,
                "winner": match.winner.value if match.winner else None,
                "created_by": str(match.created_by),
                "created_at": match.created_at,
            },
        )

    async def get_by_id(self, match_id: UUID) -> Match | None:
        row = await self._db.fetchrow(
            "SELECT * FROM matches WHERE id = :id", {"id": str(match_id)}
        )
        return _row_to_match(row) if row else None

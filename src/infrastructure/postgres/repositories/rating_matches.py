from __future__ import annotations

from uuid import UUID

import asyncpg

from domain.entities.enums.match_status import MatchStatus
from domain.entities.rating_match import RatingBout, RatingMatch, RatingPass, UserRating
from domain.ports.repositories.rating_matches import RatingMatchesRepo, UserRatingsRepo
from infrastructure.postgres.db import PostgresDB


def _row_to_match(row: asyncpg.Record) -> RatingMatch:
    return RatingMatch(
        id=row["id"],
        left_user_id=row["left_user_id"],
        right_user_id=row["right_user_id"],
        weapon_type_ids=list(row["weapon_type_ids"]),
        status=MatchStatus(row["status"]),
        winner_user_id=row["winner_user_id"],
        bouts=[],
        created_by=row["created_by"],
        created_at=row["created_at"],
        left_elo_before=float(row["left_elo_before"]) if row["left_elo_before"] is not None else None,
        right_elo_before=float(row["right_elo_before"]) if row["right_elo_before"] is not None else None,
    )


def _row_to_bout(row: asyncpg.Record) -> RatingBout:
    return RatingBout(
        id=row["id"],
        match_id=row["match_id"],
        bout_number=row["bout_number"],
        winner_user_id=row["winner_user_id"],
        passes=[],
    )


def _row_to_pass(row: asyncpg.Record) -> RatingPass:
    return RatingPass(
        id=row["id"],
        bout_id=row["bout_id"],
        pass_number=row["pass_number"],
        winner_user_id=row["winner_user_id"],
    )


class PostgresRatingMatchesRepo(RatingMatchesRepo):
    def __init__(self, db: PostgresDB) -> None:
        self._db = db

    async def save_match(self, match: RatingMatch) -> None:
        await self._db.execute(
            """
            INSERT INTO rating_matches (
                id, left_user_id, right_user_id, weapon_type_ids,
                status, winner_user_id, left_elo_before, right_elo_before,
                created_by, created_at
            ) VALUES (
                :id, :left_user_id, :right_user_id, :weapon_type_ids,
                :status, :winner_user_id, :left_elo_before, :right_elo_before,
                :created_by, :created_at
            )
            ON CONFLICT (id) DO UPDATE SET
                status           = EXCLUDED.status,
                winner_user_id   = EXCLUDED.winner_user_id,
                left_elo_before  = EXCLUDED.left_elo_before,
                right_elo_before = EXCLUDED.right_elo_before
            """,
            {
                "id": str(match.id),
                "left_user_id": str(match.left_user_id),
                "right_user_id": str(match.right_user_id),
                "weapon_type_ids": [str(wid) for wid in match.weapon_type_ids],
                "status": match.status.value,
                "winner_user_id": str(match.winner_user_id) if match.winner_user_id else None,
                "left_elo_before": match.left_elo_before,
                "right_elo_before": match.right_elo_before,
                "created_by": str(match.created_by),
                "created_at": match.created_at,
            },
        )

    async def save_bout(self, bout: RatingBout) -> None:
        await self._db.execute(
            """
            INSERT INTO rating_bouts (id, match_id, bout_number, winner_user_id)
            VALUES (:id, :match_id, :bout_number, :winner_user_id)
            ON CONFLICT (id) DO UPDATE SET
                winner_user_id = EXCLUDED.winner_user_id
            """,
            {
                "id": str(bout.id),
                "match_id": str(bout.match_id),
                "bout_number": bout.bout_number,
                "winner_user_id": str(bout.winner_user_id) if bout.winner_user_id else None,
            },
        )

    async def save_pass(self, rating_pass: RatingPass) -> None:
        await self._db.execute(
            """
            INSERT INTO rating_passes (id, bout_id, pass_number, winner_user_id)
            VALUES (:id, :bout_id, :pass_number, :winner_user_id)
            ON CONFLICT (id) DO NOTHING
            """,
            {
                "id": str(rating_pass.id),
                "bout_id": str(rating_pass.bout_id),
                "pass_number": rating_pass.pass_number,
                "winner_user_id": str(rating_pass.winner_user_id),
            },
        )

    async def get_match_by_id(self, match_id: UUID) -> RatingMatch | None:
        row = await self._db.fetchrow(
            "SELECT * FROM rating_matches WHERE id = :id", {"id": str(match_id)}
        )
        if not row:
            return None
        match = _row_to_match(row)

        bout_rows = await self._db.fetch(
            "SELECT * FROM rating_bouts WHERE match_id = :match_id ORDER BY bout_number",
            {"match_id": str(match_id)},
        )
        if not bout_rows:
            return match

        bouts = [_row_to_bout(br) for br in bout_rows]
        bout_ids = [b.id for b in bouts]

        # Single query for all passes across all bouts — avoids N+1
        pass_rows = await self._db.fetch_raw(
            "SELECT * FROM rating_passes WHERE bout_id = ANY($1::uuid[]) ORDER BY pass_number",
            bout_ids,
        )
        passes_by_bout: dict[UUID, list[RatingPass]] = {b.id: [] for b in bouts}
        for pr in pass_rows:
            passes_by_bout[pr["bout_id"]].append(_row_to_pass(pr))

        for bout in bouts:
            bout.passes = passes_by_bout[bout.id]
            match.bouts.append(bout)

        return match

    async def list_by_user(self, user_id: UUID, limit: int = 20) -> list[RatingMatch]:
        rows = await self._db.fetch(
            """
            SELECT * FROM rating_matches
            WHERE left_user_id = :user_id OR right_user_id = :user_id
            ORDER BY created_at DESC
            LIMIT :limit
            """,
            {"user_id": str(user_id), "limit": limit},
        )
        return [_row_to_match(r) for r in rows]


class PostgresUserRatingsRepo(UserRatingsRepo):
    def __init__(self, db: PostgresDB) -> None:
        self._db = db

    async def save(self, rating: UserRating) -> None:
        await self._db.execute(
            """
            INSERT INTO user_ratings (user_id, elo, matches_played)
            VALUES (:user_id, :elo, :matches_played)
            ON CONFLICT (user_id) DO UPDATE SET
                elo = EXCLUDED.elo,
                matches_played = EXCLUDED.matches_played
            """,
            {
                "user_id": str(rating.user_id),
                "elo": rating.elo,
                "matches_played": rating.matches_played,
            },
        )

    async def get_by_user_id(self, user_id: UUID) -> UserRating | None:
        row = await self._db.fetchrow(
            "SELECT * FROM user_ratings WHERE user_id = :user_id", {"user_id": str(user_id)}
        )
        if not row:
            return None
        return UserRating(
            user_id=row["user_id"],
            elo=float(row["elo"]),
            matches_played=row["matches_played"],
        )

    async def list_leaderboard(self, limit: int = 50) -> list[UserRating]:
        rows = await self._db.fetch(
            """
            SELECT * FROM user_ratings
            WHERE matches_played >= 10
            ORDER BY elo DESC
            LIMIT :limit
            """,
            {"limit": limit},
        )
        return [
            UserRating(user_id=r["user_id"], elo=float(r["elo"]), matches_played=r["matches_played"])
            for r in rows
        ]

    async def list_all_with_wins(self) -> list[tuple[UserRating, int]]:
        rows = await self._db.fetch_raw(
            """
            SELECT
                ur.user_id,
                ur.elo,
                ur.matches_played,
                COUNT(rm.id) FILTER (
                    WHERE rm.status = 'COMPLETED' AND rm.winner_user_id = ur.user_id
                )::int AS wins
            FROM user_ratings ur
            LEFT JOIN rating_matches rm
                ON rm.left_user_id = ur.user_id OR rm.right_user_id = ur.user_id
            GROUP BY ur.user_id, ur.elo, ur.matches_played
            ORDER BY ur.elo DESC
            """
        )
        return [
            (
                UserRating(user_id=r["user_id"], elo=float(r["elo"]), matches_played=r["matches_played"]),
                r["wins"],
            )
            for r in rows
        ]

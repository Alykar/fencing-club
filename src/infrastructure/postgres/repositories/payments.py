from __future__ import annotations

from uuid import UUID

import asyncpg

from domain.entities.payment import Payment
from domain.ports.repositories.payments import MonthlyPaymentRow, PaymentsRepo, UserPaymentSummary
from infrastructure.postgres.db import PostgresDB


def _row_to_payment(row: asyncpg.Record) -> Payment:
    return Payment(
        id=row["id"],
        user_id=row["user_id"],
        amount=float(row["amount"]),
        paid_at=row["paid_at"],
        processed_at=row["processed_at"],
        is_one_time=row["is_one_time"],
        note=row["note"],
        created_by=row["created_by"],
        created_at=row["created_at"],
    )


class PostgresPaymentsRepo(PaymentsRepo):
    def __init__(self, db: PostgresDB) -> None:
        self._db = db

    async def save(self, payment: Payment) -> None:
        await self._db.execute(
            """
            INSERT INTO payments (
                id, user_id, amount, paid_at, processed_at, is_one_time, note, created_by, created_at
            ) VALUES (
                :id, :user_id, :amount, :paid_at, :processed_at, :is_one_time, :note, :created_by, :created_at
            )
            ON CONFLICT (id) DO UPDATE SET
                amount = EXCLUDED.amount,
                paid_at = EXCLUDED.paid_at,
                is_one_time = EXCLUDED.is_one_time,
                note = EXCLUDED.note
            """,
            {
                "id": str(payment.id),
                "user_id": str(payment.user_id),
                "amount": payment.amount,
                "paid_at": payment.paid_at,
                "processed_at": payment.processed_at,
                "is_one_time": payment.is_one_time,
                "note": payment.note,
                "created_by": str(payment.created_by),
                "created_at": payment.created_at,
            },
        )

    async def delete(self, payment_id: UUID) -> None:
        await self._db.execute(
            "DELETE FROM payments WHERE id = :id", {"id": str(payment_id)}
        )

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        row = await self._db.fetchrow(
            "SELECT * FROM payments WHERE id = :id", {"id": str(payment_id)}
        )
        return _row_to_payment(row) if row else None

    async def list_by_user(self, user_id: UUID) -> list[Payment]:
        rows = await self._db.fetch(
            "SELECT * FROM payments WHERE user_id = :user_id ORDER BY paid_at DESC",
            {"user_id": str(user_id)},
        )
        return [_row_to_payment(r) for r in rows]

    async def list_paid_user_ids(self, user_ids: list[UUID]) -> set[UUID]:
        if not user_ids:
            return set()
        rows = await self._db.fetch_raw(
            "SELECT DISTINCT user_id FROM payments WHERE user_id = ANY($1::uuid[])",
            user_ids,
        )
        return {row["user_id"] for row in rows}

    async def list_active_subscriber_ids(
        self, user_ids: list[UUID], as_of: "date"
    ) -> set[UUID]:
        if not user_ids:
            return set()
        from datetime import date as _date  # noqa: PLC0415
        rows = await self._db.fetch_raw(
            """
            WITH last_regular AS (
                SELECT DISTINCT ON (user_id) user_id, paid_at
                FROM payments
                WHERE is_one_time = FALSE
                  AND user_id = ANY($1::uuid[])
                ORDER BY user_id, paid_at DESC
            )
            SELECT user_id
            FROM last_regular
            WHERE (paid_at + INTERVAL '1 month') >= $2
            """,
            user_ids,
            as_of,
        )
        return {row["user_id"] for row in rows}

    async def summary_all_users(self) -> list[UserPaymentSummary]:
        rows = await self._db.fetch(
            """
            SELECT
                user_id,
                SUM(amount)::float        AS total_amount,
                COUNT(*)::int             AS payments_count,
                MAX(paid_at)              AS last_paid_at,
                MAX(processed_at)         AS last_processed_at
            FROM payments
            GROUP BY user_id
            ORDER BY last_paid_at DESC NULLS LAST
            """
        )
        return [
            UserPaymentSummary(
                user_id=r["user_id"],
                total_amount=r["total_amount"],
                payments_count=r["payments_count"],
                last_paid_at=r["last_paid_at"],
                last_processed_at=r["last_processed_at"],
            )
            for r in rows
        ]

    async def monthly_summary(self, year: int, month: int) -> list[MonthlyPaymentRow]:
        rows = await self._db.fetch_raw(
            """
            WITH last_regular AS (
                SELECT DISTINCT ON (user_id)
                    user_id, paid_at AS last_regular_paid_at, amount AS last_regular_amount
                FROM payments
                WHERE is_one_time = FALSE
                ORDER BY user_id, paid_at DESC
            ),
            month_totals AS (
                SELECT user_id, SUM(amount)::float AS month_amount
                FROM payments
                WHERE EXTRACT(YEAR  FROM paid_at)::int = $1
                  AND EXTRACT(MONTH FROM paid_at)::int = $2
                GROUP BY user_id
            )
            SELECT
                COALESCE(mt.user_id, lr.user_id)  AS user_id,
                lr.last_regular_paid_at,
                lr.last_regular_amount::float,
                COALESCE(mt.month_amount, 0)       AS month_amount
            FROM month_totals mt
            FULL OUTER JOIN last_regular lr ON lr.user_id = mt.user_id
            ORDER BY mt.month_amount DESC NULLS LAST
            """,
            year, month,
        )
        return [
            MonthlyPaymentRow(
                user_id=r["user_id"],
                last_regular_paid_at=r["last_regular_paid_at"],
                last_regular_amount=float(r["last_regular_amount"]) if r["last_regular_amount"] else None,
                month_amount=float(r["month_amount"]),
            )
            for r in rows
        ]

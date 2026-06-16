from __future__ import annotations

import contextvars
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
import orjson  # noqa: F401  (registered codec side-effect)
import sqlparams

from infrastructure.postgres.config import PostgresConfig

# Stores the connection acquired for the current transaction scope.
# ContextVar ensures each asyncio Task has its own independent value,
# so concurrent requests never share a connection/transaction.
_active_conn: contextvars.ContextVar[asyncpg.Connection | None] = contextvars.ContextVar(
    "_active_conn", default=None
)


class PostgresDB:
    def __init__(self, config: PostgresConfig) -> None:
        self._config = config
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            self._config.dsn,
            min_size=self._config.pool_min_size,
            max_size=self._config.pool_max_size,
            command_timeout=self._config.command_timeout,
            max_inactive_connection_lifetime=self._config.max_inactive_connection_lifetime,
            # Enforce statement timeout at PostgreSQL level — kills runaway queries
            server_settings={
                "statement_timeout": f"{self._config.command_timeout * 1000}",
                "application_name": "fencing_club_admin",
            },
        )
        await self._pool.execute("SELECT 1")

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database is not connected. Call connect() first.")
        return self._pool

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None, None]:
        """Async context manager that wraps all DB operations in a single transaction.

        If a transaction is already active in this context (nested call), reuses it —
        the inner block participates in the outer transaction.
        """
        if _active_conn.get() is not None:
            # Already inside a transaction — participate in it without nesting
            yield
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                token = _active_conn.set(conn)
                try:
                    yield
                finally:
                    _active_conn.reset(token)

    def _conn_or_pool(self) -> asyncpg.Connection | asyncpg.Pool:
        conn = _active_conn.get()
        return conn if conn is not None else self.pool

    async def fetch(self, sql: str, params: dict | None = None) -> list[asyncpg.Record]:
        query, args = self._prepare(sql, params)
        return await self._conn_or_pool().fetch(query, *args)

    async def fetchrow(self, sql: str, params: dict | None = None) -> asyncpg.Record | None:
        query, args = self._prepare(sql, params)
        return await self._conn_or_pool().fetchrow(query, *args)

    async def execute(self, sql: str, params: dict | None = None) -> None:
        query, args = self._prepare(sql, params)
        await self._conn_or_pool().execute(query, *args)

    async def fetch_raw(self, sql: str, *args: Any) -> list[asyncpg.Record]:
        """Execute a query with raw positional asyncpg arguments.

        Use when named params cannot express the query, e.g.:
            SELECT * FROM t WHERE id = ANY($1::uuid[])
        """
        return await self._conn_or_pool().fetch(sql, *args)

    def _prepare(self, sql: str, params: dict | None) -> tuple[str, list]:
        if not params:
            return sql, []
        formatter = sqlparams.SQLParams("named", "numeric_dollar")
        query, args = formatter.format(sql, params)
        return query, list(args)

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any
from uuid import UUID

import orjson
import sqlparams
from asyncpg import Connection, Pool, Record, create_pool
from asyncpg.pool import PoolConnectionProxy

from domain.errors import TransactionRequiredError
from infrastructure.databases.database import Database, IsolationLevel, QueryOptions
from infrastructure.databases.postgres.config import PostgresConfig

logger = logging.getLogger(__name__)
_current_connection: ContextVar[Connection | None] = ContextVar("_current_connection", default=None)
_current_transaction_level: ContextVar[int] = ContextVar("_current_savepoint_number", default=0)
_current_isolation_level: ContextVar[IsolationLevel | None] = ContextVar("_current_isolation_level", default=None)


def orjson_dumps(v: Any, *, default: Callable[[Any], Any] | None = None) -> str:  # noqa: ANN401
    return orjson.dumps(
        v,
        default=default,
        option=orjson.OPT_NAIVE_UTC | orjson.OPT_NON_STR_KEYS,
    ).decode()


class PostgresDB(Database):
    """Class with configured asyncpg pool.

    Provides simpler than asyncpg interface via https://github.com/cpburnz/python-sqlparams
    """

    __slots__ = ("__pool", "__sql_param", "__default_isolation_level", "_config")

    def __init__(self) -> None:
        logger.debug("Creating database conn pool instance")
        self.__pool: Pool | None = None
        self.__sql_param = sqlparams.SQLParams("named", "numeric_dollar")
        self.__default_isolation_level: IsolationLevel = "read_committed"
        self._config: PostgresConfig | None = None

    @property
    def _pool(self) -> Pool:
        if not self.__pool:
            if self._config:
                # Создаем pool прямо здесь в текущем event loop
                import asyncio

                import nest_asyncio
                nest_asyncio.apply()

                loop = asyncio.get_event_loop()
                self.__pool = loop.run_until_complete(self._create_pool_now())
            else:
                msg = "Database connection pool was not initialized and no config provided."
                raise RuntimeError(msg)

        return self.__pool

    async def _create_pool_now(self) -> Pool:
        """Create pool immediately in current event loop."""
        if not self._config:
            msg = "No config provided for pool creation"
            raise RuntimeError(msg)

        logger.debug("Creating connection pool in current event loop")
        return await create_pool(
            dsn=str(self._config.dsn),
            init=self._connection_init(),
            setup=self._connection_setup(),
            max_size=self._config.pool_size,
            min_size=self._config.pool_size,
        )

    @property
    def in_transaction(self) -> bool:
        current_connection = _current_connection.get()

        return bool(current_connection and current_connection.is_in_transaction())

    @staticmethod
    def _connection_init() -> Callable[[Connection], Awaitable[None]]:
        logger.debug("Set encoders for types")

        async def wrapper(connection: Connection) -> None:
            await connection.set_type_codec("json", schema="pg_catalog", encoder=orjson_dumps, decoder=orjson.loads)
            await connection.set_type_codec("jsonb", schema="pg_catalog", encoder=orjson_dumps, decoder=orjson.loads)
            await connection.set_type_codec("uuid", schema="pg_catalog", encoder=str, decoder=UUID)

        return wrapper

    @staticmethod
    def _connection_setup() -> Callable[[PoolConnectionProxy], Awaitable[None]]:
        async def wrapper(connection: PoolConnectionProxy) -> None: ...  # noqa: ARG001

        return wrapper

    async def init(self, config: PostgresConfig) -> None:
        """Start connection pool."""
        logger.debug("Init connection pool")
        self._config = config
        self.__pool = await create_pool(
            dsn=str(config.dsn),
            init=self._connection_init(),
            setup=self._connection_setup(),
            max_size=config.pool_size,
            min_size=config.pool_size,
        )

    async def close(self) -> None:
        """Gracefully close database pool connections."""
        logger.debug("Close database pool")
        await self._pool.close()
        self.__pool = None

    async def fetch(
        self,
        query: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> list[Record]:
        # Простое решение: создаем новое соединение для каждого запроса
        import asyncpg

        if not self._config:
            msg = "Database config not provided"
            raise RuntimeError(msg)

        options = options or QueryOptions()

        if params:
            sql, formatted_params = self.__sql_param.format(query, params)
        else:
            sql, formatted_params = query, []

        logger.debug("Fetching with new connection", extra={"query": query})

        # Создаем новое соединение для каждого запроса
        conn = await asyncpg.connect(str(self._config.dsn))
        try:
            # Устанавливаем кодеки
            await conn.set_type_codec("json", schema="pg_catalog", encoder=orjson_dumps, decoder=orjson.loads)
            await conn.set_type_codec("jsonb", schema="pg_catalog", encoder=orjson_dumps, decoder=orjson.loads)
            await conn.set_type_codec("uuid", schema="pg_catalog", encoder=str, decoder=UUID)

            return await conn.fetch(sql, *formatted_params, timeout=options.timeout)
        finally:
            await conn.close()

    async def fetch_row(
        self,
        query: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> Record:
        import asyncpg

        if not self._config:
            msg = "Database config not provided"
            raise RuntimeError(msg)

        options = options or QueryOptions()

        if params:
            sql, formatted_params = self.__sql_param.format(query, params)
        else:
            sql, formatted_params = query, []

        logger.debug("Fetching row with new connection", extra={"query": query})

        conn = await asyncpg.connect(str(self._config.dsn))
        try:
            await conn.set_type_codec("json", schema="pg_catalog", encoder=orjson_dumps, decoder=orjson.loads)
            await conn.set_type_codec("jsonb", schema="pg_catalog", encoder=orjson_dumps, decoder=orjson.loads)
            await conn.set_type_codec("uuid", schema="pg_catalog", encoder=str, decoder=UUID)

            return await conn.fetchrow(sql, *formatted_params, timeout=options.timeout)
        finally:
            await conn.close()

    async def fetch_val(
        self,
        query: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> Any:  # noqa: ANN401
        method = getattr(_current_connection.get(), "fetchval", self._pool.fetchval)
        options = options or QueryOptions()

        if params:
            sql, formatted_params = self.__sql_param.format(query, params)
        else:
            sql, formatted_params = query, []

        logger.debug("Fetching value", extra={"query": query})
        return await method(sql, *formatted_params, timeout=options.timeout)

    async def execute(
        self,
        query: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> str:
        method: Callable[..., Awaitable[str]] = getattr(_current_connection.get(), "execute", self._pool.execute)
        options = options or QueryOptions()

        if params:
            sql, formatted_params = self.__sql_param.format(query, params)
        else:
            sql, formatted_params = query, []

        logger.debug("Executing", extra={"query": query})
        return await method(sql, *formatted_params, timeout=options.timeout)

    async def execute_many(
        self,
        query: str,
        params: Iterable[dict[str, Any]] | Iterable[Sequence[Any]] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> None:
        method = getattr(_current_connection.get(), "executemany", self._pool.executemany)
        options = options or QueryOptions()

        if params:
            sql, formatted_params = self.__sql_param.formatmany(query, params)
        else:
            sql, formatted_params = query, []

        logger.debug("Executemany", extra={"query": query})
        await method(sql, formatted_params, timeout=options.timeout)

    async def fetch_vector_similarity(
        self,
        query: str,
        vector_param: list[float],
        params: dict[str, Any] | Sequence[Any] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> list[Record]:
        """
        Выполнить запрос с векторным поиском

        Args:
            query: SQL запрос с плейсхолдером для вектора
            vector_param: Вектор для поиска схожести
            params: Дополнительные параметры запроса
            options: Опции запроса
        """
        method: Callable[..., Awaitable[list[Record]]] = getattr(_current_connection.get(), "fetch", self._pool.fetch)
        options = options or QueryOptions()

        # Подготавливаем параметры
        if params:
            sql, formatted_params = self.__sql_param.format(query, params)
        else:
            sql, formatted_params = query, []

        # Добавляем вектор как первый параметр (предполагается что в запросе он $1)
        final_params = [vector_param] + list(formatted_params)

        logger.debug("Fetching vector similarity", extra={"query": query})
        return await method(sql, *final_params, timeout=options.timeout)

    async def cursor(
        self,
        query: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
        size: int = 50,
    ) -> AsyncIterator[list[Record]]:
        if params:
            sql, formatted_params = self.__sql_param.format(query, params)
        else:
            sql, formatted_params = query, []

        logger.debug("Starting cursor", extra={"query": query})

        async with self._pool.acquire() as connection, connection.transaction():
            cursor = await connection.cursor(sql, *formatted_params)
            while rows := await cursor.fetch(size):
                yield rows

    @asynccontextmanager
    async def transaction(
        self,
        isolation: IsolationLevel | None = None,
        *,
        readonly: bool | None = None,
        deferrable: bool | None = None,
    ) -> AsyncIterator[None]:
        logger.debug("Transaction started")
        current = _current_connection.get()
        current_transaction_level = _current_transaction_level.get()

        if not current:
            logger.debug("Setting current connection")
            current = await self._pool.acquire()
            _current_connection.set(current)
            _current_isolation_level.set(isolation or self.__default_isolation_level)

            current_transaction_level += 1
            _current_transaction_level.set(current_transaction_level)
        else:
            current_transaction_level += 1
            _current_transaction_level.set(current_transaction_level)

        try:
            async with current.transaction(
                isolation=_current_isolation_level.get(),
                readonly=readonly or False,
                deferrable=deferrable or False,
            ):
                logger.debug("Starting transaction", extra={"current_level": current_transaction_level})
                yield
        finally:
            logger.debug("Transaction ended", extra={"current_level": current_transaction_level})

            current_transaction_level = _current_transaction_level.get()
            _current_transaction_level.set(current_transaction_level - 1)

            if not current.is_in_transaction():
                current = _current_connection.get()
                await self._pool.release(current)
                _current_connection.set(None)
                _current_isolation_level.set(None)

    async def lock(
        self,
        name: str,
        *,
        shared: bool | None = False,
    ) -> bool:
        if not self.in_transaction:
            raise TransactionRequiredError

        lock_name = "pg_advisory_xact_lock_shared" if shared else "pg_advisory_xact_lock"
        await self.execute(f"SELECT {lock_name}(:name)", {"name": hash(name)})

        return True

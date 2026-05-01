from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal

IsolationLevel = Literal["serializable", "repeatable_read", "read_committed"]


@dataclass
class QueryOptions:
    timeout: float | None = None


class Database(ABC):
    @property
    @abstractmethod
    def in_transaction(self) -> bool: ...

    @abstractmethod
    async def fetch(
        self,
        query: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> list[Mapping[str, Any]]:
        """Fetch all rows from database according to query."""
        ...

    @abstractmethod
    async def fetch_row(
        self,
        query: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> Mapping[str, Any]:
        """Return only one row from query result (equals LIMIT 1)."""
        ...

    @abstractmethod
    async def fetch_val(
        self,
        query: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> Any:  # noqa: ANN401
        """Fetch value from query (useful for SELECT COUNT() FROM %table%)."""
        ...

    @abstractmethod
    async def execute(
        self,
        query: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> str:
        """Execute arbitrary query, return execution result (i.e. UPDATE 0 1)."""
        ...

    @abstractmethod
    async def execute_many(
        self,
        query: str,
        params: Iterable[dict[str, Any]] | Iterable[Sequence[Any]] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> None:
        """Execute arbitrary query for list of parameters (i.e. INSERT many rows)."""
        ...

    @abstractmethod
    async def cursor(
        self,
        query: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
        size: int = 50,
    ) -> AsyncIterator[list[Mapping[str, Any]]]:
        """Return iterator for all resulting rows in query. Allows iteration over large set of data."""
        yield []

    @asynccontextmanager
    @abstractmethod
    async def transaction(
        self,
        isolation: IsolationLevel | None = None,
        *,
        readonly: bool | None = None,
        deferrable: bool | None = None,
    ) -> AsyncIterator[None]:
        """Start transaction or create savepoint inside already started transaction."""
        yield

    @abstractmethod
    async def lock(self, name: str, *, shared: bool | None = False) -> bool | None:
        """Obtain exclusive transaction level pessimistic lock.

        Lock is automatically released at the end of the current transaction and cannot be released explicitly.
        """
        ...

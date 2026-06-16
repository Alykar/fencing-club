from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager


class UnitOfWork(ABC):
    """Port for atomic multi-step operations.

    Use as an async context manager to group multiple repository calls
    into a single database transaction:

        async with self._uow.transaction():
            await repo_a.save(...)
            await repo_b.save(...)
    """

    @abstractmethod
    def transaction(self) -> AbstractAsyncContextManager[None]: ...

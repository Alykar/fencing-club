from __future__ import annotations

from contextlib import AbstractAsyncContextManager

from domain.ports.unit_of_work import UnitOfWork
from infrastructure.postgres.db import PostgresDB


class PostgresUnitOfWork(UnitOfWork):
    def __init__(self, db: PostgresDB) -> None:
        self._db = db

    def transaction(self) -> AbstractAsyncContextManager[None]:
        return self._db.transaction()

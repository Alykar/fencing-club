from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from infrastructure.databases.database import Database


class UnitOfWork:
    def __init__(self, database: Database) -> None:
        self._database = database

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        async with self._database.transaction():
            yield

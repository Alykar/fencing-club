from abc import ABC, abstractmethod
from uuid import UUID

from domain.entities.match import Match


class MatchesRepo(ABC):
    @abstractmethod
    async def save(self, match: Match) -> None: ...

    @abstractmethod
    async def get_by_id(self, match_id: UUID) -> Match | None: ...

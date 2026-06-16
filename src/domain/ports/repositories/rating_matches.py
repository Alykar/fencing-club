from abc import ABC, abstractmethod
from uuid import UUID

from domain.entities.rating_match import RatingBout, RatingMatch, RatingPass, UserRating


class RatingMatchesRepo(ABC):
    @abstractmethod
    async def save_match(self, match: RatingMatch) -> None: ...

    @abstractmethod
    async def save_bout(self, bout: RatingBout) -> None: ...

    @abstractmethod
    async def save_pass(self, rating_pass: RatingPass) -> None: ...

    @abstractmethod
    async def get_match_by_id(self, match_id: UUID) -> RatingMatch | None: ...

    @abstractmethod
    async def list_by_user(self, user_id: UUID, limit: int = 20) -> list[RatingMatch]: ...


class UserRatingsRepo(ABC):
    @abstractmethod
    async def save(self, rating: UserRating) -> None: ...

    @abstractmethod
    async def get_by_user_id(self, user_id: UUID) -> UserRating | None: ...

    @abstractmethod
    async def list_leaderboard(self, limit: int = 50) -> list[UserRating]: ...

    @abstractmethod
    async def list_all_with_wins(self) -> list[tuple[UserRating, int]]:
        """Return all user ratings paired with their completed-match win count, ordered by ELO desc."""
        ...

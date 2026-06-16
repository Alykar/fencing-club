from enum import StrEnum


class MatchStatus(StrEnum):
    ONGOING = "ongoing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

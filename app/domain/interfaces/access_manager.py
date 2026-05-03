from abc import ABC, abstractmethod

from domain.entities.user import User
from domain.utills.classes.access_keys import KeyPair


class AccessManager(ABC):

    @abstractmethod
    def grant_access(self, user: User) -> KeyPair:
        ...

    @abstractmethod
    def refresh_access(self, refresh_key: str) -> KeyPair:
        ...

    @abstractmethod
    def verify_access(self, access_key: str) -> None:
        ...


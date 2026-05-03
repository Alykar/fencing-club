from abc import ABC, abstractmethod


class PasswordManager(ABC):

    @abstractmethod
    def hash_password(self, password: str) -> str:
        pass

    @abstractmethod
    def verify_password(self, password: str, reference: str) -> None:
        pass
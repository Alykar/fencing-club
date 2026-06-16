from collections.abc import Callable
from functools import wraps
from typing import Any

from domain.entities.enums.user_role import UserRole
from domain.exceptions import ForbiddenError, UserBlockedError
from domain.ports.access_manager import AuthUser

_ROLE_LEVEL = {UserRole.USER: 0, UserRole.INSTRUCTOR: 1, UserRole.ADMIN: 2}


def require_role(minimum_role: UserRole) -> Callable:
    """Декоратор для методов use case — проверяет role и is_blocked у AuthUser.

    Ожидает, что первый аргумент после self — это AuthUser (или он передан как kwarg auth_user).
    """
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            auth_user: AuthUser | None = kwargs.get("auth_user")
            if auth_user is None:
                for arg in args:
                    if isinstance(arg, AuthUser):
                        auth_user = arg
                        break

            if auth_user is None:
                raise ForbiddenError("auth_user not provided")

            if auth_user.is_blocked:
                raise UserBlockedError(f"User {auth_user.id} is blocked")

            if _ROLE_LEVEL.get(auth_user.role, -1) < _ROLE_LEVEL.get(minimum_role, 99):
                raise ForbiddenError(
                    f"Required role {minimum_role}, got {auth_user.role}"
                )

            return await fn(self, *args, **kwargs)

        return wrapper

    return decorator

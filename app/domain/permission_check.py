import functools
import inspect
from typing import Callable

from domain.errors import ForbiddenError
import functools
import inspect
from typing import Callable

from domain.entities.enums.user_role import UserRole
from domain.errors import ForbiddenError


def require_auth(*allowed_roles: UserRole):
    def decorator(func):
        sig = inspect.signature(func)

        if 'auth_user' not in sig.parameters:
            raise TypeError(
                f"Method {func.__qualname__} mast have param 'auth_user'"
            )

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            auth_user = bound.arguments['auth_user']

            if auth_user.role not in allowed_roles:
                raise ForbiddenError

            return await func(*args, **kwargs)

        return async_wrapper

    return decorator
from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from fastapi import Cookie, Depends, HTTPException, Request
from lagom import Container
from starlette.status import HTTP_303_SEE_OTHER

from domain.entities.enums.user_role import UserRole
from domain.ports.access_manager import AuthUser

T = TypeVar("T")

_ROLE_LEVEL = {UserRole.USER: 0, UserRole.INSTRUCTOR: 1, UserRole.ADMIN: 2}

_LOGIN_REDIRECT = HTTPException(
    status_code=HTTP_303_SEE_OTHER,
    headers={"Location": "/auth/register"},
)
_PENDING_REDIRECT = HTTPException(
    status_code=HTTP_303_SEE_OTHER,
    headers={"Location": "/auth/pending"},
)


def require_role(minimum_role: UserRole) -> Callable[[AuthUser], AuthUser]:
    """FastAPI dependency — raises 403 if the authenticated user has insufficient role."""

    async def _check(auth_user: AuthUser = Depends(get_auth_user)) -> AuthUser:
        if _ROLE_LEVEL.get(auth_user.role, -1) < _ROLE_LEVEL[minimum_role]:
            raise HTTPException(status_code=403, detail="Недостаточно прав")
        return auth_user

    _check.__name__ = f"require_role_{minimum_role.value}"
    return _check  # type: ignore[return-value]


def get_container(request: Request) -> Container:
    return request.app.state.container


def depends(dep_type: type[T]) -> Callable[[Container], T]:
    """Return a FastAPI dependency that resolves *dep_type* from the DI container.

    Usage::

        use_case: ListUsersUseCase = Depends(depends(ListUsersUseCase))
    """
    def resolver(container: Container = Depends(get_container)) -> T:
        return container.resolve(dep_type)

    # Give the inner function a unique name so FastAPI caches it per dep_type,
    # not as a single shared closure.
    resolver.__name__ = f"resolve_{dep_type.__name__}"
    return resolver


async def get_auth_user(
    access_token: str | None = Cookie(default=None),
    container: Container = Depends(get_container),
) -> AuthUser:
    """Decode auth user from JWT — O(1), no DB round-trip.

    is_blocked and is_access_granted are embedded in the token at login time.
    The token's short TTL (1 h) limits the exposure window if a user is blocked
    after login. For immediate revocation, use the refresh-token flow or
    implement a short-lived token blocklist.
    """
    if not access_token:
        raise _LOGIN_REDIRECT

    from domain.ports.access_manager import AccessManager

    access_manager: AccessManager = container[AccessManager]

    try:
        auth_user = access_manager.decode_access_token(access_token)
    except Exception as exc:
        raise _LOGIN_REDIRECT from exc

    if auth_user.is_blocked:
        raise _LOGIN_REDIRECT

    if not auth_user.is_access_granted:
        raise _PENDING_REDIRECT

    return auth_user

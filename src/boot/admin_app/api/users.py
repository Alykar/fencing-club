from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER

from boot.admin_app.deps import depends, require_role
from domain.entities.enums.user_role import UserRole
from domain.ports.access_manager import AuthUser
from use_cases.user.grant_hall_access import GrantHallAccessUseCase
from use_cases.user.list_pending_members import ListPendingMembersUseCase
from use_cases.user.list_users import ListUsersUseCase
from use_cases.user.update_permissions import UpdatePermissionsInput, UpdatePermissionsUseCase

router = APIRouter(tags=["users"])
_templates: Jinja2Templates | None = None


def set_templates(t: Jinja2Templates) -> None:
    global _templates
    _templates = t


def templates() -> Jinja2Templates:
    if _templates is None:
        raise RuntimeError("Templates not configured — call set_templates() during app setup")
    return _templates


@router.get("/users", response_class=HTMLResponse)
async def users_list(
    request: Request,
    q: str = "",
    auth_user: AuthUser = Depends(require_role(UserRole.ADMIN)),
    use_case: ListUsersUseCase = Depends(depends(ListUsersUseCase)),
):
    users = await use_case(auth_user, query=q or None)
    return templates().TemplateResponse(
        request, "users/list.html", {"users": users, "auth_user": auth_user, "query": q}
    )


@router.get("/users/pending", response_class=HTMLResponse)
async def pending_members(
    request: Request,
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    use_case: ListPendingMembersUseCase = Depends(depends(ListPendingMembersUseCase)),
):
    members = await use_case(auth_user)
    return templates().TemplateResponse(
        request, "users/pending.html", {"members": members, "auth_user": auth_user}
    )


@router.post("/users/{user_id}/grant-access")
async def grant_access(
    user_id: UUID,
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    use_case: GrantHallAccessUseCase = Depends(depends(GrantHallAccessUseCase)),
):
    await use_case(auth_user, user_id)
    return RedirectResponse(url="/users/pending", status_code=HTTP_303_SEE_OTHER)


@router.post("/users/{user_id}/permissions")
async def update_permissions(
    user_id: UUID,
    role: UserRole = Form(...),
    is_blocked: bool = Form(False),
    auth_user: AuthUser = Depends(require_role(UserRole.ADMIN)),
    use_case: UpdatePermissionsUseCase = Depends(depends(UpdatePermissionsUseCase)),
):
    await use_case(auth_user, user_id, UpdatePermissionsInput(role=role, is_blocked=is_blocked))
    return RedirectResponse(url="/users", status_code=HTTP_303_SEE_OTHER)

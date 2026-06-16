from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from boot.admin_app.deps import get_auth_user
from domain.ports.access_manager import AuthUser

router = APIRouter(tags=["tabata"])
_templates: Jinja2Templates | None = None


def set_templates(t: Jinja2Templates) -> None:
    global _templates
    _templates = t


def templates() -> Jinja2Templates:
    if _templates is None:
        raise RuntimeError("Templates not configured — call set_templates() during app setup")
    return _templates


@router.get("/tabata", response_class=HTMLResponse)
async def tabata_page(
    request: Request,
    auth_user: AuthUser = Depends(get_auth_user),
):
    return templates().TemplateResponse(request, "tabata/tabata.html", {"auth_user": auth_user})

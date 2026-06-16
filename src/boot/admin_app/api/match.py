"""Match router — pure frontend, no DB storage.

The match is managed entirely in the browser via JavaScript.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from boot.admin_app.deps import get_auth_user
from domain.ports.access_manager import AuthUser

router = APIRouter(tags=["match"])
_templates: Jinja2Templates | None = None


def set_templates(t: Jinja2Templates) -> None:
    global _templates
    _templates = t


def templates() -> Jinja2Templates:
    if _templates is None:
        raise RuntimeError("Templates not configured — call set_templates() during app setup")
    return _templates


@router.get("/match", response_class=HTMLResponse)
async def match_hub(
    request: Request,
    auth_user: AuthUser = Depends(get_auth_user),
):
    return templates().TemplateResponse(request, "match/hub.html", {"auth_user": auth_user})


@router.get("/match/start", response_class=HTMLResponse)
async def match_page(
    request: Request,
    auth_user: AuthUser = Depends(get_auth_user),
):
    return templates().TemplateResponse(request, "match/match.html", {"auth_user": auth_user})

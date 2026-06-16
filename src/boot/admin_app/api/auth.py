from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER

from boot.admin_app.deps import depends
from domain.entities.enums.referral_source import ReferralSource
from domain.exceptions import (
    PasswordMismatchError,
    PasswordsDoNotMatchError,
    UserAlreadyExistsError,
    UserBlockedError,
    UserNotFoundError,
)
from domain.ports.access_manager import KeyPair
from use_cases.auth.login import LoginInput, LoginUseCase
from use_cases.auth.register import RegisterInput, RegisterUseCase

router = APIRouter(tags=["auth"])
_templates: Jinja2Templates | None = None


def set_templates(t: Jinja2Templates) -> None:
    global _templates
    _templates = t


def templates() -> Jinja2Templates:
    if _templates is None:
        raise RuntimeError("Templates not configured — call set_templates() during app setup")
    return _templates


def _secure(request: Request) -> bool:
    return bool(getattr(request.app.state, "secure_cookies", False))


def _set_auth_cookies(resp: RedirectResponse, tokens: KeyPair, secure: bool) -> None:
    resp.set_cookie(
        "access_token", tokens.access_token, httponly=True, samesite="lax", secure=secure
    )
    resp.set_cookie(
        "refresh_token", tokens.refresh_token, httponly=True, samesite="lax", secure=secure
    )


@router.get("/auth/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates().TemplateResponse(request, "auth/login.html", {"error": None})


@router.post("/auth/login")
async def login(
    request: Request,
    response: Response,
    email: str = Form(...),
    password: str = Form(...),
    use_case: LoginUseCase = Depends(depends(LoginUseCase)),
):
    try:
        tokens = await use_case(LoginInput(email=email, password=password))
    except (UserNotFoundError, PasswordMismatchError):
        return templates().TemplateResponse(
            request, "auth/login.html", {"error": "Неверный email или пароль"}, status_code=401
        )
    except UserBlockedError:
        return templates().TemplateResponse(
            request, "auth/login.html", {"error": "Аккаунт заблокирован"}, status_code=403
        )

    secure = _secure(request)
    resp = RedirectResponse(url="/admin/profile", status_code=HTTP_303_SEE_OTHER)
    _set_auth_cookies(resp, tokens, secure)
    return resp


@router.get("/auth/pending", response_class=HTMLResponse)
async def pending_page(request: Request):
    return templates().TemplateResponse(request, "auth/pending.html", {})


@router.get("/auth/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates().TemplateResponse(
        request, "auth/register.html", {"error": None, "referral_sources": list(ReferralSource)}
    )


@router.post("/auth/register")
async def register(
    request: Request,
    last_name: str = Form(...),
    first_name: str = Form(...),
    patronymic: str = Form(""),
    tel: str = Form(...),
    email: str = Form(...),
    tg: str = Form(""),
    vk: str = Form(""),
    password: str = Form(...),
    password_confirm: str = Form(...),
    referral_source: ReferralSource = Form(...),
    friend_name: str = Form(""),
    use_case: RegisterUseCase = Depends(depends(RegisterUseCase)),
):
    try:
        await use_case(
            RegisterInput(
                last_name=last_name,
                first_name=first_name,
                patronymic=patronymic or None,
                tel=tel,
                email=email,
                tg=tg or None,
                vk=vk or None,
                password=password,
                password_confirm=password_confirm,
                referral_source=referral_source,
                friend_name=friend_name or None,
            )
        )
    except PasswordsDoNotMatchError:
        error = "Пароли не совпадают"
    except UserAlreadyExistsError:
        error = "Пользователь с таким email уже существует"
    except ValueError as exc:
        error = str(exc)
    else:
        return RedirectResponse(url="/admin/auth/pending", status_code=HTTP_303_SEE_OTHER)

    return templates().TemplateResponse(
        request,
        "auth/register.html",
        {"error": error, "referral_sources": list(ReferralSource)},
        status_code=422,
    )


@router.post("/auth/logout")
async def logout(request: Request):
    secure = _secure(request)
    resp = RedirectResponse(url="/admin/auth/login", status_code=HTTP_303_SEE_OTHER)
    resp.delete_cookie("access_token", httponly=True, samesite="lax", secure=secure)
    resp.delete_cookie("refresh_token", httponly=True, samesite="lax", secure=secure)
    return resp

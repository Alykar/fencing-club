from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from lagom import Container
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from boot.admin_app.api import (
    attendance,
    auth,
    match,
    payment,
    profile,
    rating,
    schedule,
    tabata,
    users,
)

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).parent / "templates"
_STATIC_DIR = Path(__file__).parent / "static"

_WEEKDAY_LABELS = {
    "monday": "Пн", "tuesday": "Вт", "wednesday": "Ср",
    "thursday": "Чт", "friday": "Пт", "saturday": "Сб", "sunday": "Вс",
}
_KIND_LABELS = {
    "longsword": "Длинный меч", "sabre": "Военная сабля",
    "sparring": "Свободные спарринги", "event": "Ивент", "other": "Прочее",
}
_ROLE_LABELS = {
    "user": "Участник", "instructor": "Инструктор", "admin": "Администратор",
}

_SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "X-XSS-Protection": "1; mode=block",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Permissions-Policy": "geolocation=(), microphone=(), camera=()",
    # Restrict content sources: only same-origin, CDN for HTMX/Tailwind,
    # no inline scripts (mitigates XSS). Adjust cdn sources if self-hosting.
    "Content-Security-Policy": (
        "default-src 'self'; "
        "script-src 'self' https://unpkg.com https://cdn.tailwindcss.com 'unsafe-inline'; "
        "style-src 'self' https://cdn.tailwindcss.com 'unsafe-inline'; "
        "font-src 'self'; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "frame-ancestors 'none';"
    ),
}


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: object) -> Response:
        response: Response = await call_next(request)  # type: ignore[arg-type]
        for header, value in _SECURITY_HEADERS.items():
            response.headers[header] = value
        return response


class PendingCountMiddleware(BaseHTTPMiddleware):
    """Inject pending_users_count into request.state for every admin HTML page."""

    async def dispatch(self, request: Request, call_next: object) -> Response:
        request.state.pending_count = 0
        # Only run for authenticated HTML pages; skip auth and static routes
        path = request.url.path
        if not path.startswith("/admin/auth") and not path.startswith("/static"):
            try:
                from domain.ports.repositories.users import UsersRepo
                container: Container = request.app.state.container
                users_repo: UsersRepo = container[UsersRepo]
                request.state.pending_count = await users_repo.count_pending()
            except Exception:  # noqa: BLE001 S110
                pass  # Never break a page because of this
        response: Response = await call_next(request)  # type: ignore[arg-type]
        return response


_ERROR_META = {
    400: ("Некорректный запрос",  "Запрос содержит ошибки. Проверьте данные и попробуйте снова."),
    403: ("Доступ запрещён",      "У вас недостаточно прав для просмотра этой страницы."),
    404: ("Страница не найдена",  "Запрошенная страница не существует или была перемещена."),
    405: ("Метод не разрешён",    "Этот адрес не поддерживает данный HTTP-метод."),
    422: ("Ошибка валидации",     "Данные формы содержат ошибки. Проверьте введённую информацию."),
    500: ("Внутренняя ошибка",    "На сервере возникла непредвиденная ошибка. Попробуйте позже."),
}


def _make_error_handler(tpl: Jinja2Templates):
    async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> HTMLResponse:
        code = exc.status_code
        # Non-HTML requests (HTMX, etc.) — return plain response
        accept = request.headers.get("accept", "")
        if "text/html" not in accept and "hx-request" not in request.headers:
            return Response(content=str(exc.detail), status_code=code)

        default = ("Ошибка", str(exc.detail) or "Что-то пошло не так.")
        title, message = _ERROR_META.get(code, default)
        if code != 404:
            logger.warning("HTTP %d at %s — %s", code, request.url.path, exc.detail)

        # Try to get auth_user from token for the base template
        auth_user = None
        try:
            from lagom import Container as LagomContainer

            from domain.ports.access_manager import AccessManager
            c: LagomContainer = request.app.state.container
            am: AccessManager = c[AccessManager]
            token = request.cookies.get("access_token")
            if token:
                auth_user = am.decode_access_token(token)
        except Exception:  # noqa: BLE001 S110
            pass

        return tpl.TemplateResponse(
            request,
            "errors/error.html",
            {"code": code, "title": title, "message": message, "auth_user": auth_user},
            status_code=code,
        )

    async def validation_error_handler(  # noqa: E501
        request: Request, exc: RequestValidationError
    ) -> HTMLResponse:
        title, message = _ERROR_META[422]
        return tpl.TemplateResponse(
            request,
            "errors/error.html",
            {"code": 422, "title": title, "message": message, "auth_user": None},
            status_code=422,
        )

    return http_exception_handler, validation_error_handler


def setup_admin(app: FastAPI, container: Container, *, secure_cookies: bool = False) -> None:
    app.state.container = container
    app.state.secure_cookies = secure_cookies

    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(PendingCountMiddleware)

    def _to_dmy(value: object) -> str:
        """Convert date/YYYY-MM-DD string to DD/MM/YYYY for display in form inputs."""
        from datetime import date as _date
        if isinstance(value, _date):
            return value.strftime("%d/%m/%Y")
        if isinstance(value, str) and len(value) == 10 and value[4] == "-":
            return f"{value[8:10]}/{value[5:7]}/{value[0:4]}"
        return str(value)

    _WEEKDAY_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

    def _weekday_short(value: object) -> str:
        from datetime import datetime as _dt, date as _date
        if isinstance(value, (_dt, _date)):
            return _WEEKDAY_SHORT[value.weekday()]
        return str(value)

    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    templates.env.filters["weekday_label"] = lambda v: _WEEKDAY_LABELS.get(str(v), str(v))
    templates.env.filters["weekday_short"] = _weekday_short
    templates.env.filters["kind_label"] = lambda v: _KIND_LABELS.get(str(v), str(v))
    templates.env.filters["role_label"] = lambda v: _ROLE_LABELS.get(str(v), str(v))
    templates.env.filters["to_dmy"] = _to_dmy

    http_exc_handler, validation_exc_handler = _make_error_handler(templates)
    app.add_exception_handler(StarletteHTTPException, http_exc_handler)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, validation_exc_handler)  # type: ignore[arg-type]

    for module in (auth, profile, users, attendance, payment, schedule, match, rating, tabata):
        module.set_templates(templates)

    prefix = "/admin"
    app.include_router(auth.router, prefix=prefix)
    app.include_router(profile.router, prefix=prefix)
    app.include_router(users.router, prefix=prefix)
    app.include_router(attendance.router, prefix=prefix)
    app.include_router(payment.router, prefix=prefix)
    app.include_router(schedule.router, prefix=prefix)
    app.include_router(match.router, prefix=prefix)
    app.include_router(rating.router, prefix=prefix)
    app.include_router(tabata.router, prefix=prefix)

    if _STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    @app.get("/")
    async def root() -> RedirectResponse:
        return RedirectResponse(url="/admin/profile")

    @app.get("/admin")
    async def admin_root() -> RedirectResponse:
        return RedirectResponse(url="/admin/profile")

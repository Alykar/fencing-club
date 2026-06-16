from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER

from boot.admin_app.deps import depends, get_auth_user
from domain.entities.training_session import TrainingSession
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.rating_matches import UserRatingsRepo
from domain.ports.repositories.training_sessions import TrainingSessionsRepo
from use_cases.user.get_profile import GetProfileUseCase
from use_cases.user.update_profile import UpdateProfileInput, UpdateProfileUseCase

router = APIRouter(tags=["profile"])
_templates: Jinja2Templates | None = None

_RU_WEEKDAYS = ("Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье")
_RU_WEEKDAYS_SHORT = ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")
_RU_MONTHS_SHORT = ("янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек")


def set_templates(t: Jinja2Templates) -> None:
    global _templates
    _templates = t


def templates() -> Jinja2Templates:
    if _templates is None:
        raise RuntimeError("Templates not configured — call set_templates() during app setup")
    return _templates


def _build_week(
    week_offset: int,
    sessions: list[TrainingSession],
) -> tuple[list[dict], date, date]:
    """Return list of day-dicts and (week_start, week_end) dates."""
    today = date.today()
    week_start = today - timedelta(days=today.weekday()) + timedelta(weeks=week_offset)
    week_end = week_start + timedelta(days=6)

    by_date: defaultdict[date, list[TrainingSession]] = defaultdict(list)
    for s in sessions:
        # starts_at is UTC-aware; get the local date via UTC date (good enough without TZ config)
        by_date[s.starts_at.date()].append(s)

    week_days = []
    for i in range(7):
        d = week_start + timedelta(days=i)
        day_sessions = sorted(by_date.get(d, []), key=lambda s: s.starts_at)
        week_days.append({
            "date": d,
            "weekday": _RU_WEEKDAYS[i],
            "weekday_short": _RU_WEEKDAYS_SHORT[i],
            "month_label": f"{d.day} {_RU_MONTHS_SHORT[d.month - 1]}",
            "is_today": d == today,
            "is_weekend": i >= 5,
            "sessions": day_sessions,
        })

    return week_days, week_start, week_end


@router.get("/profile", response_class=HTMLResponse)
async def profile_page(
    request: Request,
    week_offset: int = Query(0),
    auth_user: AuthUser = Depends(get_auth_user),
    get_profile: GetProfileUseCase = Depends(depends(GetProfileUseCase)),
    sessions_repo: TrainingSessionsRepo = Depends(depends(TrainingSessionsRepo)),
    ratings_repo: UserRatingsRepo = Depends(depends(UserRatingsRepo)),
):
    user = await get_profile(auth_user.id)
    rating = await ratings_repo.get_by_user_id(auth_user.id)

    # Load sessions for the target week (±1 day buffer for TZ edge cases)
    today = date.today()
    week_start_d = today - timedelta(days=today.weekday()) + timedelta(weeks=week_offset)
    week_end_d = week_start_d + timedelta(days=7)
    from_dt = datetime(week_start_d.year, week_start_d.month, week_start_d.day, tzinfo=UTC)
    to_dt = datetime(week_end_d.year, week_end_d.month, week_end_d.day, tzinfo=UTC)
    sessions = await sessions_repo.list_by_date_range(from_dt, to_dt)

    week_days, week_start, week_end = _build_week(week_offset, sessions)

    return templates().TemplateResponse(
        request,
        "profile/profile.html",
        {
            "user": user,
            "auth_user": auth_user,
            "rating": rating,
            "week_days": week_days,
            "week_offset": week_offset,
            "week_start": week_start,
            "week_end": week_end,
        },
    )


@router.post("/profile")
async def update_profile(
    request: Request,
    name: str = Form(...),
    tel: str = Form(...),
    tg: str = Form(""),
    vk: str = Form(""),
    auth_user: AuthUser = Depends(get_auth_user),
    use_case: UpdateProfileUseCase = Depends(depends(UpdateProfileUseCase)),
):
    await use_case(
        auth_user,
        auth_user.id,
        UpdateProfileInput(name=name, tel=tel, tg=tg or None, vk=vk or None),
    )
    return RedirectResponse(url="/admin/profile", status_code=HTTP_303_SEE_OTHER)

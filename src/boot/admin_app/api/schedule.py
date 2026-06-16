from __future__ import annotations

from datetime import date, time
from uuid import UUID

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER

from boot.admin_app.deps import depends, require_role
from domain.entities.enums.user_role import UserRole
from domain.entities.enums.weekday import Weekday
from domain.ports.access_manager import AuthUser
from use_cases.schedule.create_schedule import CreateScheduleInput, CreateScheduleUseCase
from use_cases.schedule.deactivate_schedule import DeactivateScheduleUseCase
from use_cases.schedule.list_active_schedules import ListActiveSchedulesUseCase
from use_cases.schedule.populate_sessions import PopulateSessionsForMonthUseCase

router = APIRouter(tags=["schedule"])
_templates: Jinja2Templates | None = None


def set_templates(t: Jinja2Templates) -> None:
    global _templates
    _templates = t


def templates() -> Jinja2Templates:
    if _templates is None:
        raise RuntimeError("Templates not configured — call set_templates() during app setup")
    return _templates


@router.get("/schedule", response_class=HTMLResponse)
async def schedule_page(
    request: Request,
    auth_user: AuthUser = Depends(require_role(UserRole.ADMIN)),
    list_schedules: ListActiveSchedulesUseCase = Depends(depends(ListActiveSchedulesUseCase)),
):
    schedules = await list_schedules()
    return templates().TemplateResponse(
        request,
        "schedule/manage.html",
        {
            "schedules": schedules,
            "auth_user": auth_user,
            "weekdays": list(Weekday),
        },
    )


@router.post("/schedule")
async def create_schedule(
    request: Request,
    title: str = Form(...),
    description: str = Form(""),
    weekdays: list[Weekday] = Form(default=[]),
    time_of_day: time = Form(...),
    duration_minutes: int = Form(90),
    location: str = Form(...),
    populate_from: date = Form(...),
    populate_to: date | None = Form(None),
    auth_user: AuthUser = Depends(require_role(UserRole.ADMIN)),
    use_case: CreateScheduleUseCase = Depends(depends(CreateScheduleUseCase)),
):
    await use_case(
        auth_user,
        CreateScheduleInput(
            title=title,
            description=description or None,
            weekdays=weekdays,
            time_of_day=time_of_day,
            duration_minutes=duration_minutes,
            location=location,
            populate_from=populate_from,
            populate_to=populate_to or None,
        ),
    )
    return RedirectResponse(url="/schedule", status_code=HTTP_303_SEE_OTHER)


@router.post("/schedule/populate")
async def populate_sessions(
    auth_user: AuthUser = Depends(require_role(UserRole.ADMIN)),
    use_case: PopulateSessionsForMonthUseCase = Depends(depends(PopulateSessionsForMonthUseCase)),
):
    """Manually trigger session generation for next 2 months. Idempotent."""
    from use_cases.schedule.create_schedule import _next_month

    today = date.today()
    m0_first = date(today.year, today.month, 1)
    m1_first = _next_month(m0_first)
    m2_first = _next_month(m1_first)

    count = 0
    for first in (m0_first, m1_first, m2_first):
        count += await use_case(first.year, first.month)

    return RedirectResponse(
        url=f"/schedule?populated={count}",
        status_code=HTTP_303_SEE_OTHER,
    )


@router.post("/schedule/{schedule_id}/deactivate")
async def deactivate_schedule(
    schedule_id: UUID,
    auth_user: AuthUser = Depends(require_role(UserRole.ADMIN)),
    use_case: DeactivateScheduleUseCase = Depends(depends(DeactivateScheduleUseCase)),
):
    await use_case(auth_user, schedule_id)
    return RedirectResponse(url="/schedule", status_code=HTTP_303_SEE_OTHER)

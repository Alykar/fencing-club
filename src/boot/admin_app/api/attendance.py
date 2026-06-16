from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER

from boot.admin_app.deps import depends, require_role
from domain.entities.enums.user_role import UserRole
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.attendances import AttendancesRepo
from domain.ports.repositories.payments import PaymentsRepo
from domain.ports.repositories.training_sessions import TrainingSessionsRepo
from use_cases.attendance.list_session_attendance import ListSessionAttendanceUseCase
from use_cases.attendance.mark_attendance import MarkAttendanceUseCase
from use_cases.attendance.unmark_attendance import UnmarkAttendanceUseCase
from use_cases.user.list_users import ListUsersUseCase

router = APIRouter(tags=["attendance"])
_templates: Jinja2Templates | None = None


def set_templates(t: Jinja2Templates) -> None:
    global _templates
    _templates = t


def templates() -> Jinja2Templates:
    if _templates is None:
        raise RuntimeError("Templates not configured — call set_templates() during app setup")
    return _templates


@router.get("/attendance", response_class=HTMLResponse)
async def attendance_sessions(
    request: Request,
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    sessions_repo: TrainingSessionsRepo = Depends(depends(TrainingSessionsRepo)),
    attendances_repo: AttendancesRepo = Depends(depends(AttendancesRepo)),
):
    today = date.today()
    # Показываем весь текущий месяц: с 1-го числа по конец
    month_start = datetime(today.year, today.month, 1, tzinfo=timezone.utc)
    if today.month == 12:
        month_end = datetime(today.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        month_end = datetime(today.year, today.month + 1, 1, tzinfo=timezone.utc)

    now = datetime.now(tz=timezone.utc)
    sessions = await sessions_repo.list_by_date_range(month_start, month_end)
    sessions = [s for s in sessions if s.starts_at <= now]
    sessions.sort(key=lambda s: s.starts_at, reverse=True)

    session_ids = [s.id for s in sessions]
    counts = await attendances_repo.count_by_sessions(session_ids)
    unique_this_month = await attendances_repo.count_unique_in_month(today.year, today.month)

    return templates().TemplateResponse(
        request,
        "attendance/sessions.html",
        {
            "sessions": sessions,
            "counts": counts,
            "unique_this_month": unique_this_month,
            "month_name": _month_ru(today.month),
            "today": today,
            "auth_user": auth_user,
        },
    )


_MONTHS_RU = [
    "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
]


def _month_ru(month: int) -> str:
    return _MONTHS_RU[month]


@router.get("/attendance/{session_id}", response_class=HTMLResponse)
async def attendance_mark(
    request: Request,
    session_id: UUID,
    q: str = "",
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    sessions_repo: TrainingSessionsRepo = Depends(depends(TrainingSessionsRepo)),
    list_users: ListUsersUseCase = Depends(depends(ListUsersUseCase)),
    list_attendance: ListSessionAttendanceUseCase = Depends(depends(ListSessionAttendanceUseCase)),
    payments_repo: PaymentsRepo = Depends(depends(PaymentsRepo)),
):
    session = await sessions_repo.get_by_id(session_id)
    if session and session.starts_at.date() > date.today():
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Нельзя отмечать посещаемость для будущих тренировок")

    users = await list_users(auth_user, query=q or None)
    attendances = await list_attendance(auth_user, session_id)
    attended_ids = {a.user_id for a in attendances}

    user_ids = [u.id for u in users]
    active_ids = await payments_repo.list_active_subscriber_ids(user_ids, date.today())
    payments_by_user: dict[UUID, bool] = {u.id: u.id in active_ids for u in users}

    return templates().TemplateResponse(
        request,
        "attendance/mark.html",
        {
            "session_id": session_id,
            "users": users,
            "attended_ids": attended_ids,
            "payments_by_user": payments_by_user,
            "auth_user": auth_user,
            "query": q,
        },
    )


@router.post("/attendance/{session_id}/mark/{user_id}")
async def mark(
    session_id: UUID,
    user_id: UUID,
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    use_case: MarkAttendanceUseCase = Depends(depends(MarkAttendanceUseCase)),
):
    await use_case(auth_user, session_id, user_id)
    return RedirectResponse(url=f"/attendance/{session_id}", status_code=HTTP_303_SEE_OTHER)


@router.post("/attendance/{session_id}/unmark/{user_id}")
async def unmark(
    session_id: UUID,
    user_id: UUID,
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    use_case: UnmarkAttendanceUseCase = Depends(depends(UnmarkAttendanceUseCase)),
):
    await use_case(auth_user, session_id, user_id)
    return RedirectResponse(url=f"/attendance/{session_id}", status_code=HTTP_303_SEE_OTHER)

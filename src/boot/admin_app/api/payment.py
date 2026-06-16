from __future__ import annotations

from datetime import date
from uuid import UUID
import calendar

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER

from boot.admin_app.deps import depends, require_role
from domain.entities.enums.user_role import UserRole
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.attendances import AttendancesRepo
from domain.ports.repositories.payments import PaymentsRepo
from domain.ports.repositories.users import UsersRepo
from use_cases.payment.create_payment import CreatePaymentInput, CreatePaymentUseCase
from use_cases.payment.delete_payment import DeletePaymentUseCase

router = APIRouter(tags=["payment"])
_templates: Jinja2Templates | None = None

_RU_MONTHS = [
    "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
]


def _add_month(d: date) -> date:
    """Return date + exactly 1 calendar month (capped to last day of month)."""
    m = d.month + 1
    y = d.year
    if m > 12:
        m = 1
        y += 1
    day = min(d.day, calendar.monthrange(y, m)[1])
    return date(y, m, day)


def _paid_until_status(paid_until: date | None, today: date) -> str:
    """Return 'expired' | 'expiring' (≤7 days) | 'active' | 'none'."""
    if paid_until is None:
        return "none"
    if paid_until < today:
        return "expired"
    if (paid_until - today).days <= 7:
        return "expiring"
    return "active"


def set_templates(t: Jinja2Templates) -> None:
    global _templates
    _templates = t


def templates() -> Jinja2Templates:
    if _templates is None:
        raise RuntimeError("Templates not configured — call set_templates() during app setup")
    return _templates


@router.get("/payment", response_class=HTMLResponse)
async def payment_page(
    request: Request,
    month: int | None = Query(default=None),
    year: int | None = Query(default=None),
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    users_repo: UsersRepo = Depends(depends(UsersRepo)),
    payments_repo: PaymentsRepo = Depends(depends(PaymentsRepo)),
    attendances_repo: AttendancesRepo = Depends(depends(AttendancesRepo)),
):
    today = date.today()
    sel_month = month if month and 1 <= month <= 12 else today.month
    sel_year = year if year and 2020 <= year <= 2100 else today.year

    # Month tabs: current, -1, -2
    def _prev(y: int, m: int, n: int = 1) -> tuple[int, int]:
        m -= n
        while m < 1:
            m += 12
            y -= 1
        return y, m

    tabs = []
    for delta, label in [(0, "Текущий"), (-1, None), (-2, None)]:
        ty, tm = _prev(today.year, today.month, -delta)
        tabs.append({
            "year": ty, "month": tm,
            "label": label or _RU_MONTHS[tm],
            "active": (ty == sel_year and tm == sel_month),
        })

    # Load data — full users for table (need role), select-items for form dropdown
    all_users = await users_repo.list_all()
    select_users = [u for u in all_users if u.has_hall_pass and not u.is_blocked]
    table_users = [u for u in all_users if u.has_hall_pass and not u.is_blocked]

    monthly_rows = await payments_repo.monthly_summary(sel_year, sel_month)
    attendance_counts = await attendances_repo.count_by_users_in_month(sel_year, sel_month)

    _role_order = {"admin": 0, "instructor": 1, "user": 2}

    # Build display rows — all active members, sorted by role → attendance ↓ → month_amount ↓
    rows_by_uid = {r.user_id: r for r in monthly_rows}
    table_rows = []
    for user in table_users:
        row = rows_by_uid.get(user.id)
        lrpa = row.last_regular_paid_at if row else None
        paid_until = _add_month(lrpa) if lrpa else None
        attendance = attendance_counts.get(user.id, 0)
        month_amount = row.month_amount if row else 0.0
        table_rows.append({
            "user_id": user.id,
            "name": user.name,
            "role": user.role.value,
            "paid_until": paid_until,
            "paid_until_status": _paid_until_status(paid_until, today),
            "month_amount": month_amount,
            "attendance": attendance,
            "_sort_key": (_role_order.get(user.role.value, 9), -attendance, -month_amount),
        })

    table_rows.sort(key=lambda r: r["_sort_key"])

    # Stats
    total_month_amount = sum(r["month_amount"] for r in table_rows)
    total_attendance = sum(r["attendance"] for r in table_rows)
    paid_count = sum(1 for r in table_rows if r["month_amount"] > 0)

    is_admin = auth_user.role.value == "admin"
    return templates().TemplateResponse(
        request,
        "payment/manage.html",
        {
            "users": select_users,
            "today": today.isoformat(),
            "auth_user": auth_user,
            "is_admin": is_admin,
            "tabs": tabs,
            "sel_month": sel_month,
            "sel_year": sel_year,
            "sel_month_name": _RU_MONTHS[sel_month],
            "table_rows": table_rows,
            "total_month_amount": total_month_amount,
            "total_attendance": total_attendance,
            "paid_count": paid_count,
            "members_count": len(table_rows),
        },
    )


@router.post("/payment")
async def create_payment(
    request: Request,
    user_id: UUID = Form(...),
    amount: float = Form(...),
    paid_at: date = Form(...),
    is_one_time: bool = Form(False),
    note: str = Form(""),
    auth_user: AuthUser = Depends(require_role(UserRole.ADMIN)),
    use_case: CreatePaymentUseCase = Depends(depends(CreatePaymentUseCase)),
):
    await use_case(
        auth_user,
        CreatePaymentInput(
            user_id=user_id,
            amount=amount,
            paid_at=paid_at,
            is_one_time=is_one_time,
            note=note or None,
        ),
    )
    return RedirectResponse(url="/admin/payment", status_code=HTTP_303_SEE_OTHER)


@router.post("/payment/{payment_id}/delete")
async def delete_payment(
    payment_id: UUID,
    auth_user: AuthUser = Depends(require_role(UserRole.ADMIN)),
    use_case: DeletePaymentUseCase = Depends(depends(DeletePaymentUseCase)),
):
    await use_case(auth_user, payment_id)
    return RedirectResponse(url="/admin/payment", status_code=HTTP_303_SEE_OTHER)

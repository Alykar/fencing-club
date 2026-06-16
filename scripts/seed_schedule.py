"""Seed training schedules and generate sessions.

Schedules (recurring, open-ended, from Feb 2024):
  - ПН  Военная сабля       20:00  105 min
  - СР  Длинный меч         20:00  105 min
  - СБ  Свободные спарринги 12:00  120 min
  - ВС  Свободные спарринги 14:30  120 min

Sessions generated: 2024-02-01 … 2026-12-31
"""
from __future__ import annotations

import asyncio
import os
from datetime import UTC, date, datetime, time, timedelta
from uuid import uuid4

import asyncpg

# ── Schedule definitions ──────────────────────────────────────────────────────
SCHEDULES = [
    {
        "title": "Военная сабля",
        "kind": "sabre",
        "weekday": 0,        # Monday
        "time": time(20, 0),
        "duration": 105,
    },
    {
        "title": "Длинный меч",
        "kind": "longsword",
        "weekday": 2,        # Wednesday
        "time": time(20, 0),
        "duration": 105,
    },
    {
        "title": "Свободные спарринги",
        "kind": "sparring",
        "weekday": 5,        # Saturday
        "time": time(12, 0),
        "duration": 120,
    },
    {
        "title": "Свободные спарринги",
        "kind": "sparring",
        "weekday": 6,        # Sunday
        "time": time(14, 30),
        "duration": 120,
    },
]

POPULATE_FROM = date(2024, 2, 1)
POPULATE_TO   = date(2026, 12, 31)
LOCATION      = "Зал ULTRA"


def _combine(d: date, t: time) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute, tzinfo=UTC)


def _generate_sessions(
    schedule_id: str,
    weekday: int,
    t: time,
    duration: int,
    title: str,
    kind: str,
    created_by: str,
    now: datetime,
) -> list[dict]:
    sessions = []
    current = POPULATE_FROM
    while current <= POPULATE_TO:
        if current.weekday() == weekday:
            sessions.append({
                "id": str(uuid4()),
                "schedule_id": schedule_id,
                "title": title,
                "kind": kind,
                "starts_at": _combine(current, t),
                "duration_minutes": duration,
                "location": LOCATION,
                "description": None,
                "instructor_id": None,
                "created_by": created_by,
                "updated_by": created_by,
                "created_at": now,
            })
        current += timedelta(days=1)
    return sessions


async def main() -> None:
    dsn = (
        f"postgresql://{os.environ['POSTGRES__USER']}:{os.environ['POSTGRES__PASSWORD']}"
        f"@{os.environ.get('POSTGRES__HOST','localhost')}:{os.environ.get('POSTGRES__PORT','5432')}"
        f"/{os.environ['POSTGRES__DB']}"
    )
    conn = await asyncpg.connect(dsn)

    admin_row = await conn.fetchrow(
        "SELECT id FROM users WHERE role = 'admin' ORDER BY created_at LIMIT 1"
    )
    if not admin_row:
        print("ERROR: No admin user found.")
        await conn.close()
        return
    admin_id = str(admin_row["id"])
    now = datetime.now(UTC)

    # Clear existing schedules and sessions
    await conn.execute("DELETE FROM training_sessions")
    await conn.execute("DELETE FROM training_schedules")
    print("Cleared existing schedules and sessions.")

    total_sessions = 0

    async with conn.transaction():
        for sched in SCHEDULES:
            sched_id = str(uuid4())

            # Insert schedule
            await conn.execute("""
                INSERT INTO training_schedules (
                    id, title, description, kind, status, weekdays,
                    time_of_day, duration_minutes, location,
                    instructor_id, cost_per_session,
                    populate_from, populate_to, created_by, created_at
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
                )
            """,
                sched_id,
                sched["title"],
                None,
                sched["kind"],
                "active",
                # weekday enum string
                [["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][sched["weekday"]]],
                sched["time"],
                sched["duration"],
                LOCATION,
                None,
                None,
                POPULATE_FROM,
                None,   # open-ended
                admin_id,
                now,
            )

            # Generate sessions
            sessions = _generate_sessions(
                sched_id, sched["weekday"], sched["time"],
                sched["duration"], sched["title"], sched["kind"],
                admin_id, now,
            )

            # Bulk insert sessions
            await conn.executemany("""
                INSERT INTO training_sessions (
                    id, schedule_id, title, description, kind,
                    starts_at, duration_minutes, location,
                    instructor_id, created_by, updated_by, created_at
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
                )
                ON CONFLICT (schedule_id, starts_at) DO NOTHING
            """, [
                (
                    s["id"], s["schedule_id"], s["title"], s["description"],
                    s["kind"], s["starts_at"], s["duration_minutes"],
                    s["location"], s["instructor_id"],
                    s["created_by"], s["updated_by"], s["created_at"],
                )
                for s in sessions
            ])

            total_sessions += len(sessions)
            wd_name = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"][sched["weekday"]]
            print(f"  ✓ {sched['title']} ({wd_name} {sched['time'].strftime('%H:%M')}) → {len(sessions)} тренировок")

    print(f"\n✓ Расписаний: {len(SCHEDULES)}")
    print(f"✓ Тренировок всего: {total_sessions}")
    print(f"  Период: {POPULATE_FROM} — {POPULATE_TO}")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

"""Seed members and their payment history from CSV export.

Usage (from repo root):
    docker compose exec admin python /app/scripts/seed_members.py
"""
from __future__ import annotations

import asyncio
import re
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import asyncpg
import bcrypt

# ── CSV path (mounted as volume ./src → /app/src, CSV in repo root) ──────────
CSV_PATH = Path(__file__).parent.parent / "Смета УЛЬТРА - Люди и оплата.csv"

# ── Month columns (index in CSV → actual date) ────────────────────────────────
MONTH_COLS: list[tuple[int, date]] = [
    (3,  date(2024, 2, 1)),   # Февраль 2024
    (4,  date(2024, 3, 1)),   # Март 2024
    (5,  date(2024, 4, 1)),   # Апрель 2024
    (6,  date(2024, 9, 1)),   # Сентябрь 2024
    (7,  date(2024, 10, 1)),  # Октябрь 2024
    (8,  date(2024, 11, 1)),  # Ноябрь 2024
    (9,  date(2024, 12, 1)),  # Декабрь 2024
    (10, date(2025, 1, 1)),   # Январь 2025
    (11, date(2025, 2, 1)),   # Февраль 2025
    (12, date(2025, 3, 1)),   # Март 2025
    (13, date(2025, 4, 1)),   # Апрель 2025
    (14, date(2025, 5, 1)),   # Май 2025
    (15, date(2025, 6, 1)),   # Июнь 2025
    (16, date(2025, 7, 1)),   # Июль 2025
]

# Section headers to skip
_SKIP_NAMES = {
    "TOTAL", "Все сборы", "Доп Доходы", "Все расходы", "Доп Расходы",
    "Аренда Зала", "ФИО", "Сертификаты и прочее",
    "Постоянники", "Новички", "Отпавшие / Нерегулярные",
}

# ── Transliteration ───────────────────────────────────────────────────────────
_TR: dict[str, str] = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'yo','ж':'zh',
    'з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o',
    'п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts',
    'ч':'ch','ш':'sh','щ':'sch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
}

def _translit(text: str) -> str:
    out = []
    for ch in text.lower():
        out.append(_TR.get(ch, ch))
    return "".join(out)


def _make_email(name: str, existing: set[str]) -> str:
    """Generate unique email from Russian name."""
    parts = name.strip().split()
    # Use first two words: Фамилия Имя (or just first if single word)
    slug = "_".join(_translit(p) for p in parts[:2] if p)
    # Remove non-alphanumeric except underscore
    slug = re.sub(r"[^a-z0-9_]", "", slug)
    base = f"{slug}@ultra-fencing.club"
    candidate = base
    i = 2
    while candidate in existing:
        candidate = f"{slug}{i}@ultra-fencing.club"
        i += 1
    existing.add(candidate)
    return candidate


def _parse_amount(raw: str) -> float | None:
    """Parse 'р.2 000,00' → 2000.0, or None if empty/zero."""
    raw = raw.strip()
    if not raw:
        return None
    # Remove 'р.' prefix, spaces, replace comma decimal
    cleaned = raw.replace("р.", "").replace("\u00a0", "").replace(" ", "").replace(",", ".")
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def _parse_tg(social: str) -> str | None:
    if not social:
        return None
    m = re.search(r"@\S+", social)
    return m.group(0) if m else None


_DEFAULT_PASSWORD = "Ultra2024!"
_DEFAULT_HASH = bcrypt.hashpw(_DEFAULT_PASSWORD.encode(), bcrypt.gensalt(rounds=12)).decode()


async def main() -> None:
    import os
    dsn = (
        f"postgresql://{os.environ['POSTGRES__USER']}:{os.environ['POSTGRES__PASSWORD']}"
        f"@{os.environ.get('POSTGRES__HOST','localhost')}:{os.environ.get('POSTGRES__PORT','5432')}"
        f"/{os.environ['POSTGRES__DB']}"
    )
    conn = await asyncpg.connect(dsn)

    # Get admin id for created_by
    admin_row = await conn.fetchrow(
        "SELECT id FROM users WHERE role = 'admin' ORDER BY created_at LIMIT 1"
    )
    if not admin_row:
        print("ERROR: No admin user found. Create superadmin first.")
        await conn.close()
        sys.exit(1)
    admin_id = str(admin_row["id"])
    print(f"Using admin id: {admin_id}")

    # Existing emails to avoid duplicates
    rows = await conn.fetch("SELECT email FROM users")
    existing_emails: set[str] = {r["email"] for r in rows}

    # Parse CSV (proper quoting support)
    import csv
    import io
    text = CSV_PATH.read_text(encoding="utf-8")
    reader = csv.reader(io.StringIO(text))
    all_rows = list(reader)

    users_inserted = 0
    payments_inserted = 0
    now = datetime.now(UTC)

    async with conn.transaction():
        for cols in all_rows:
            name = cols[0].strip() if cols else ""

            if not name or name in _SKIP_NAMES:
                continue
            # Skip aggregate/total rows
            if not any(c.isalpha() for c in name):
                continue

            tel = cols[1].strip() if len(cols) > 1 else ""
            social = cols[2].strip() if len(cols) > 2 else ""

            tg = _parse_tg(social)
            email = _make_email(name, existing_emails)
            user_id = str(uuid4())

            await conn.execute("""
                INSERT INTO users (
                    id, name, tel, email, tg, vk, role, password_hash,
                    is_access_granted, has_hall_pass, is_blocked,
                    referral_source, friend_name, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                ON CONFLICT DO NOTHING
            """,
                user_id, name, tel or "", email,
                tg, None, "user", _DEFAULT_HASH,
                True, True, False,
                "other", None, now,
            )
            users_inserted += 1

            # Parse payments
            for col_idx, paid_at in MONTH_COLS:
                if col_idx >= len(cols):
                    continue
                amount = _parse_amount(cols[col_idx])
                if amount is None:
                    continue

                paid_dt = datetime(paid_at.year, paid_at.month, paid_at.day, 12, 0, 0, tzinfo=UTC)
                await conn.execute("""
                    INSERT INTO payments (
                        id, user_id, amount, paid_at, processed_at,
                        is_one_time, note, created_by, created_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                """,
                    str(uuid4()), user_id, amount, paid_at, paid_dt,
                    False, None, admin_id, now,
                )
                payments_inserted += 1

    print(f"✓ Добавлено пользователей: {users_inserted}")
    print(f"✓ Добавлено платежей:      {payments_inserted}")
    print(f"  Пароль по умолчанию:     {_DEFAULT_PASSWORD}")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

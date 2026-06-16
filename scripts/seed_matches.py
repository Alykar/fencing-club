"""Seed rating matches from matches.json into the database.

Run from repo root:
    docker compose exec admin python /app/scripts/seed_matches.py
"""
from __future__ import annotations

import asyncio
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4, UUID

import asyncpg

# ─── Config ──────────────────────────────────────────────────────────────────
DB_DSN = "postgresql://fencing:fencing_pass_2026@db:5432/fencing_club"
MATCHES_FILE = Path(__file__).parent.parent / "matches.json"

ELO_DEFAULT = 1000.0
MAX_CONVINCING_SCORE = 1.3
GLICKO_BASE_K = 30.0
GLICKO_MIN_RD = 50.0
GLICKO_MAX_RD = 350.0

# ─── Name → user_id mapping ──────────────────────────────────────────────────
# Короткие имена/позывные из matches.json → точные имена в БД
NAME_MAP: dict[str, str] = {
    "Никита":   "Никита Антипенко",
    "Паша":     "Паша Ерофеев",
    "ПашаБ":    "Паша Параваев",
    "Аркаша":   "Аркадий Зайцев",
    "Данил":    "Данил Степанчук",
    "Гоша":     "Георгий Голубцов",
    "Давид":    "Давид Багдасарян",
    "Демид":    "Демид Синицкий",
    "Кирилл":   "Кирилл Шмидт",
    "Миша":     "Михаил Авакумов",
    "Сема":     "Семен Горохов",
    "Сергей":   "Сергей Васильев",
    "Тимофей":  "Тимофей Федоров",
    "Фима":     "Фима",
}

# ─── Weapon name normalization ────────────────────────────────────────────────
def normalize_weapon(name: str) -> str:
    n = name.strip().lower()
    if n in ("лонг", "лонги", "полутор"):
        return "Длинный меч"
    if n in ("сабля", "саблч", "сабли", "сабля 80", "сабля соло"):
        return "Сабля"
    if n in ("щит меч", "щит-меч", "щит/меч", "щит:меч", "щитмеч", "шит меч",
             "меч щит", "меч-щит", "меч:щит", "щитмеч"):
        return "Меч и щит"
    if n in ("меч баклер", "меч/баклер", "меч:баклер", "м/б"):
        return "Меч и баклер"
    if n in ("сабля баклер", "сабля-баклер", "сабля/баклер", "сабля:баклер",
             "сабляباклер", "сабляباклер"):
        return "Сабля с баклером"
    if n in ("парные мечи", "парные", "2 меча", "два меча", "два-меча", "дуалы"):
        return "Парные мечи"
    if n in ("копье", "копья", "копьё", "копьч"):
        return "Копьё"
    if n in ("копье-щит", "копьё-щит", "копья-щит"):
        return "Копьё и щит"
    if n == "алебарда":
        return "Алебарда"
    if n in ("одноруч", "меч"):
        return "Одноручный меч"
    if n == "катана":
        return "Катана"
    if n in ("щит+сабля", "щит сабля"):
        return "Сабля с щитом"
    # Дополнительные варианты написания
    if "сабля" in n and "баклер" in n:
        return "Сабля с баклером"
    if "меч" in n and "баклер" in n:
        return "Меч и баклер"
    if ("щит" in n and "меч" in n) or ("меч" in n and "щит" in n):
        return "Меч и щит"
    # Fallback — оставляем как есть с заглавной буквы
    return name.strip().capitalize()


def calc_rd(matches_played: int) -> float:
    rd = GLICKO_MAX_RD * math.exp(-matches_played / 10.0) + GLICKO_MIN_RD
    return min(GLICKO_MAX_RD, max(GLICKO_MIN_RD, rd))


def calc_elo(elo: float, opponent_elo_before: float, convincing_score: float, matches_played: int, won: bool) -> float:
    rd = calc_rd(matches_played)
    k = GLICKO_BASE_K * (rd / 100.0)
    rating_diff = elo - opponent_elo_before
    win_prob = 1.0 / (1.0 + 10.0 ** (-rating_diff / 400.0))
    actual_norm = convincing_score / MAX_CONVINCING_SCORE
    change = k * (actual_norm - win_prob)
    if won and change < 0:
        change = 0.0
    return round(elo + change, 2)


async def main() -> None:
    data = json.loads(MATCHES_FILE.read_text(encoding="utf-8"))
    conn = await asyncpg.connect(DB_DSN)

    try:
        # ── 0. Очищаем старые данные ───────────────────────────────────────
        await conn.execute("DELETE FROM rating_passes")
        await conn.execute("DELETE FROM rating_bouts")
        await conn.execute("DELETE FROM rating_matches")
        await conn.execute("DELETE FROM user_ratings")
        print("✓ Старые рейтинговые данные очищены")

        # ── 1. Получаем users ──────────────────────────────────────────────
        rows = await conn.fetch("SELECT id, name FROM users")
        name_to_id: dict[str, UUID] = {r["name"]: r["id"] for r in rows}

        user_ids: dict[str, UUID] = {}
        missing: list[str] = []
        for alias, full_name in NAME_MAP.items():
            uid = name_to_id.get(full_name)
            if uid:
                user_ids[alias] = uid
            else:
                missing.append(f"  {alias!r} → {full_name!r} — НЕ НАЙДЕН")

        if missing:
            print("⚠  Не найдены пользователи:")
            print("\n".join(missing))
            print("Продолжаем с теми, кто найден...")

        # ── 2. Получаем/создаём weapon_types ──────────────────────────────
        weapon_rows = await conn.fetch("SELECT id, name FROM weapon_types")
        weapon_id_map: dict[str, UUID] = {r["name"]: r["id"] for r in weapon_rows}

        # Собираем все нужные оружия
        needed_weapons: set[str] = set()
        for match in data:
            for fight_key in ("fight_1", "fight_2", "fight_3"):
                needed_weapons.add(normalize_weapon(match[fight_key]["weapon"]))

        for wname in sorted(needed_weapons):
            if wname not in weapon_id_map:
                wid = uuid4()
                await conn.execute(
                    "INSERT INTO weapon_types (id, name) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING",
                    str(wid), wname,
                )
                # Перечитываем (ON CONFLICT мог не вставить)
                row = await conn.fetchrow("SELECT id FROM weapon_types WHERE name = $1", wname)
                weapon_id_map[wname] = row["id"]
                print(f"  + Оружие: {wname}")

        # ── 3. Загружаем текущий ELO ──────────────────────────────────────
        elo_rows = await conn.fetch("SELECT user_id, elo, matches_played FROM user_ratings")
        elos: dict[UUID, float] = {r["user_id"]: float(r["elo"]) for r in elo_rows}
        played: dict[UUID, int] = {r["user_id"]: r["matches_played"] for r in elo_rows}

        def get_elo(uid: UUID) -> float:
            return elos.get(uid, ELO_DEFAULT)

        def get_played(uid: UUID) -> int:
            return played.get(uid, 0)

        # ── 4. Создаём матчи ──────────────────────────────────────────────
        created_at_base = datetime(2024, 2, 1, 18, 0, 0, tzinfo=timezone.utc)
        created = 0
        skipped = 0

        for i, match_data in enumerate(data):
            f1_alias = match_data["fighter_1"]
            f2_alias = match_data["fighter_2"]

            if f1_alias not in user_ids or f2_alias not in user_ids:
                print(f"  skip матч #{i+1}: {f1_alias} vs {f2_alias} — участник не найден")
                skipped += 1
                continue

            left_uid = user_ids[f1_alias]
            right_uid = user_ids[f2_alias]

            # Weapon type IDs для 3 боёв
            wids = []
            for fight_key in ("fight_1", "fight_2", "fight_3"):
                wname = normalize_weapon(match_data[fight_key]["weapon"])
                wids.append(str(weapon_id_map[wname]))

            match_id = uuid4()
            # Считаем результаты боёв и определяем победителя матча
            bout_winners: list[UUID] = []
            bout_data: list[dict] = []
            for bi, fight_key in enumerate(("fight_1", "fight_2", "fight_3")):
                fd = match_data[fight_key]
                l_sc, r_sc = fd["l_score"], fd["r_score"]
                # Победитель боя — у кого больше сходов
                bout_winner = left_uid if l_sc > r_sc else right_uid
                bout_winners.append(bout_winner)
                bout_data.append({"l": l_sc, "r": r_sc, "winner": bout_winner})

            left_bout_wins = sum(1 for w in bout_winners if w == left_uid)
            right_bout_wins = sum(1 for w in bout_winners if w == right_uid)
            match_winner = left_uid if left_bout_wins >= right_bout_wins else right_uid

            # ELO снимки до матча
            left_elo_before = get_elo(left_uid)
            right_elo_before = get_elo(right_uid)

            # Convincing score (как в calculate_scores доменной модели)
            left_pts = 0.0
            right_pts = 0.0
            for bd in bout_data:
                left_pts  += bd["l"] * 0.1   # за сходы
                right_pts += bd["r"] * 0.1
                if bd["winner"] == left_uid:
                    left_pts += 0.1
                else:
                    right_pts += 0.1
            if match_winner == left_uid:
                left_pts += 0.1
            else:
                right_pts += 0.1

            left_won = match_winner == left_uid
            # Используем pre-match ELO для обоих (фикс бага)
            new_left_elo  = calc_elo(left_elo_before,  right_elo_before, left_pts,  get_played(left_uid),  won=left_won)
            new_right_elo = calc_elo(right_elo_before, left_elo_before,  right_pts, get_played(right_uid), won=not left_won)

            created_at = created_at_base.replace(
                year=2024 + (i // 30),
                month=((i // 4) % 12) + 1,
                day=min(28, (i % 4) * 7 + 1),
            )

            async with conn.transaction():
                # INSERT match
                await conn.execute(
                    """
                    INSERT INTO rating_matches
                        (id, left_user_id, right_user_id, weapon_type_ids, status,
                         winner_user_id, created_by, created_at,
                         left_elo_before, right_elo_before)
                    VALUES ($1,$2,$3,$4,'COMPLETED',$5,$6,$7,$8,$9)
                    ON CONFLICT DO NOTHING
                    """,
                    str(match_id), str(left_uid), str(right_uid),
                    wids, str(match_winner),
                    str(left_uid),   # created_by = left fighter
                    created_at,
                    left_elo_before, right_elo_before,
                )

                # INSERT bouts + passes
                for bi, bd in enumerate(bout_data):
                    bout_id = uuid4()
                    await conn.execute(
                        """
                        INSERT INTO rating_bouts
                            (id, match_id, bout_number, winner_user_id)
                        VALUES ($1,$2,$3,$4)
                        ON CONFLICT DO NOTHING
                        """,
                        str(bout_id), str(match_id), bi + 1, str(bd["winner"]),
                    )
                    # passes: l_score раз побеждает left, r_score раз побеждает right
                    pass_num = 1
                    for _ in range(bd["l"]):
                        await conn.execute(
                            "INSERT INTO rating_passes (id, bout_id, pass_number, winner_user_id) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                            str(uuid4()), str(bout_id), pass_num, str(left_uid),
                        )
                        pass_num += 1
                    for _ in range(bd["r"]):
                        await conn.execute(
                            "INSERT INTO rating_passes (id, bout_id, pass_number, winner_user_id) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                            str(uuid4()), str(bout_id), pass_num, str(right_uid),
                        )
                        pass_num += 1

                # UPDATE ELO
                for uid, new_elo, new_played in [
                    (left_uid,  new_left_elo,  get_played(left_uid)  + 1),
                    (right_uid, new_right_elo, get_played(right_uid) + 1),
                ]:
                    await conn.execute(
                        """
                        INSERT INTO user_ratings (user_id, elo, matches_played)
                        VALUES ($1,$2,$3)
                        ON CONFLICT (user_id) DO UPDATE SET
                            elo = EXCLUDED.elo,
                            matches_played = EXCLUDED.matches_played
                        """,
                        str(uid), new_elo, new_played,
                    )
                    elos[uid] = new_elo
                    played[uid] = new_played

            created += 1

        print(f"\n✓ Создано матчей: {created}  /  пропущено: {skipped}")
        print("\nИтоговый рейтинг:")
        for alias, uid in sorted(user_ids.items(), key=lambda x: -elos.get(x[1], ELO_DEFAULT)):
            print(f"  {NAME_MAP[alias]:25s}  ELO={elos.get(uid, ELO_DEFAULT):.0f}  матчей={played.get(uid, 0)}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

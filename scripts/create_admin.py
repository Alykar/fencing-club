"""One-shot script: create a super-admin user.

Usage:
    PYTHONPATH=src python scripts/create_admin.py
"""
from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime

import asyncpg
import bcrypt


async def main() -> None:
    dsn = (
        f"postgresql://{os.environ['POSTGRES__USER']}:{os.environ['POSTGRES__PASSWORD']}"
        f"@{os.environ.get('POSTGRES__HOST', 'localhost')}:{os.environ.get('POSTGRES__PORT', '5432')}"
        f"/{os.environ['POSTGRES__DB']}"
    )

    email = os.environ.get("ADMIN_EMAIL", "admin@ultra-fencing.club")
    password = os.environ.get("ADMIN_PASSWORD", "Ultra2026!")
    name = os.environ.get("ADMIN_NAME", "Супер Администратор")

    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    user_id = str(uuid.uuid4())
    now = datetime.now(UTC)

    conn = await asyncpg.connect(dsn)
    try:
        existing = await conn.fetchrow("SELECT id, role FROM users WHERE email = $1", email)
        if existing:
            await conn.execute(
                "UPDATE users SET role = 'admin', is_blocked = FALSE, is_access_granted = TRUE WHERE email = $1",
                email,
            )
            print(f"✓ Existing user promoted to admin: {email}")
        else:
            await conn.execute(
                """
                INSERT INTO users (
                    id, name, tel, email, tg, vk, role, password_hash,
                    is_access_granted, is_blocked, referral_source, friend_name, created_at
                ) VALUES ($1,$2,$3,$4,NULL,NULL,'admin',$5,TRUE,FALSE,'other',NULL,$6)
                """,
                user_id, name, "+70000000000", email, pw_hash, now,
            )
            print(f"✓ Super-admin created: {email}")
    finally:
        await conn.close()

    print()
    print("=" * 40)
    print(f"  Email   : {email}")
    print(f"  Password: {password}")
    print(f"  Role    : admin")
    print("=" * 40)


asyncio.run(main())

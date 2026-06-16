from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta

import uvicorn
from fastapi import FastAPI

from boot.settings import Settings

logger = logging.getLogger(__name__)


async def _session_populate_job(container: object) -> None:
    """Daily background task. On the 1st of each month generates sessions 2 months ahead."""
    from use_cases.schedule.populate_sessions import (
        PopulateSessionsForMonthUseCase,
        target_month_for_populate,
    )

    # Resolve use case once; it holds no state
    use_case: PopulateSessionsForMonthUseCase = container[PopulateSessionsForMonthUseCase]  # type: ignore[index]

    while True:
        now = datetime.now(UTC)
        # Next run: 00:05 UTC the following day
        tomorrow_0005 = (now + timedelta(days=1)).replace(
            hour=0, minute=5, second=0, microsecond=0
        )
        sleep_seconds = (tomorrow_0005 - now).total_seconds()
        logger.debug("session_populate_job sleeping %.0fs until %s", sleep_seconds, tomorrow_0005)
        await asyncio.sleep(sleep_seconds)

        today = date.today()
        if today.day != 1:
            continue

        year, month = target_month_for_populate(today)
        try:
            count = await use_case(year, month)
            logger.info(
                "Monthly session populate completed year=%d month=%02d count=%d",
                year, month, count,
            )
        except Exception:
            logger.exception("Monthly session populate failed year=%d month=%02d", year, month)


def create_app() -> FastAPI:
    from boot.admin_app.app import setup_admin
    from boot.container import build_container

    settings = Settings()

    from infrastructure.postgres.db import PostgresDB

    db = PostgresDB(settings.postgres)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await db.connect()
        logger.info("Database connected")

        container = app.state.container
        task = asyncio.create_task(_session_populate_job(container))
        logger.info("Session populate background job started")

        yield

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await db.disconnect()
        logger.info("Database disconnected")

    container = build_container(db, settings.jwt)
    app = FastAPI(title="Fencing Club", lifespan=lifespan)
    setup_admin(app, container, secure_cookies=settings.secure_cookies)

    return app


def main() -> None:
    settings = Settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    app = create_app()
    uvicorn.run(app, host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()

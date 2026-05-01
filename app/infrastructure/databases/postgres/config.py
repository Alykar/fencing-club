from typing import Literal

from pydantic import BaseModel, PostgresDsn


class PostgresConfig(BaseModel):
    dsn: PostgresDsn
    isolation_level: Literal["serializable", "repeatable_read", "read_committed"] = "read_committed"
    pool_size: int = 10

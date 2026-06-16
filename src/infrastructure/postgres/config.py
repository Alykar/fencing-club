from pydantic import BaseModel, SecretStr


class PostgresConfig(BaseModel):
    host: str = "localhost"
    port: int = 5432
    user: str
    password: SecretStr
    db: str
    echo: bool = False

    # Connection pool
    pool_min_size: int = 5
    pool_max_size: int = 20
    # Seconds before asyncpg raises asyncpg.exceptions.QueryCanceledError
    command_timeout: int = 30
    # Seconds before idle connections are closed
    max_inactive_connection_lifetime: int = 300

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.db}"
        )

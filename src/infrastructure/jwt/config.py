from pydantic import BaseModel, SecretStr, field_validator


class JWTConfig(BaseModel):
    secret: SecretStr
    algorithm: str = "HS256"
    access_token_ttl_seconds: int = 3600
    refresh_token_ttl_seconds: int = 604800

    @field_validator("secret")
    @classmethod
    def secret_min_length(cls, v: SecretStr) -> SecretStr:
        if len(v.get_secret_value()) < 32:
            raise ValueError("JWT secret must be at least 32 characters long")
        return v

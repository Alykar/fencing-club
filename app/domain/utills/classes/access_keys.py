from pydantic import BaseModel


class KeyPair(BaseModel):
    access: str
    refresh: str

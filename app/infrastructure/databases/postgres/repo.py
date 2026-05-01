from infrastructure.databases.postgres.db import PostgresDB


class PostgresDBRepo:
    def __init__(self, db: PostgresDB):
        self.db = db

from lagom import Container

from domain.ports.access_manager import AccessManager
from domain.ports.password_manager import PasswordManager
from domain.ports.repositories.attendances import AttendancesRepo
from domain.ports.repositories.matches import MatchesRepo
from domain.ports.repositories.payments import PaymentsRepo
from domain.ports.repositories.rating_matches import RatingMatchesRepo, UserRatingsRepo
from domain.ports.repositories.training_schedules import TrainingSchedulesRepo
from domain.ports.repositories.training_sessions import TrainingSessionsRepo
from domain.ports.repositories.users import UsersRepo
from domain.ports.repositories.weapon_types import WeaponTypesRepo
from domain.ports.unit_of_work import UnitOfWork
from infrastructure.bcrypt.service import BcryptPasswordManager
from infrastructure.jwt.config import JWTConfig
from infrastructure.jwt.service import JWTAccessManager
from infrastructure.postgres.db import PostgresDB
from infrastructure.postgres.repositories.attendances import PostgresAttendancesRepo
from infrastructure.postgres.repositories.matches import PostgresMatchesRepo
from infrastructure.postgres.repositories.payments import PostgresPaymentsRepo
from infrastructure.postgres.repositories.rating_matches import (
    PostgresRatingMatchesRepo,
    PostgresUserRatingsRepo,
)
from infrastructure.postgres.repositories.training_schedules import PostgresTrainingSchedulesRepo
from infrastructure.postgres.repositories.training_sessions import PostgresTrainingSessionsRepo
from infrastructure.postgres.repositories.users import PostgresUsersRepo
from infrastructure.postgres.repositories.weapon_types import PostgresWeaponTypesRepo
from infrastructure.postgres.unit_of_work import PostgresUnitOfWork


def build_container(db: PostgresDB, jwt_config: JWTConfig) -> Container:
    container = Container()

    container[UsersRepo] = PostgresUsersRepo(db)
    container[TrainingSessionsRepo] = PostgresTrainingSessionsRepo(db)
    container[TrainingSchedulesRepo] = PostgresTrainingSchedulesRepo(db)
    container[AttendancesRepo] = PostgresAttendancesRepo(db)
    container[PaymentsRepo] = PostgresPaymentsRepo(db)
    container[MatchesRepo] = PostgresMatchesRepo(db)
    container[RatingMatchesRepo] = PostgresRatingMatchesRepo(db)
    container[UserRatingsRepo] = PostgresUserRatingsRepo(db)
    container[WeaponTypesRepo] = PostgresWeaponTypesRepo(db)

    container[UnitOfWork] = PostgresUnitOfWork(db)

    container[AccessManager] = JWTAccessManager(jwt_config)
    container[PasswordManager] = BcryptPasswordManager()

    return container

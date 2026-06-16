class DomainError(Exception):
    pass


class AdditionalContactRequiredError(DomainError):
    pass


class PasswordsDoNotMatchError(DomainError):
    pass


class UserAlreadyExistsError(DomainError):
    pass


class UserNotFoundError(DomainError):
    pass


class UserBlockedError(DomainError):
    pass


class PasswordMismatchError(DomainError):
    pass


class ForbiddenError(DomainError):
    pass


class TrainingSessionNotFoundError(DomainError):
    pass


class TrainingScheduleNotFoundError(DomainError):
    pass


class LocationIsEmptyError(DomainError):
    pass


class CannotCreatePastTrainingSessionError(DomainError):
    pass


class TrainingIsPastDueEditWindowError(DomainError):
    pass


class InvalidDatesError(DomainError):
    pass


class ScheduleWeekdaysMissingError(DomainError):
    pass


class MatchNotFoundError(DomainError):
    pass


class MatchAlreadyCompletedError(DomainError):
    pass


class RatingMatchNotFoundError(DomainError):
    pass


class WeaponTypeNotFoundError(DomainError):
    pass


class PaymentNotFoundError(DomainError):
    pass

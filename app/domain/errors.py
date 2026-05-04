

class AdditionalContactRequiredError(Exception):
    pass

class PasswordsDoNotMatchError(Exception):
    pass

class UserAlreadyExistsError(Exception):
    pass

class UserNotFoundError(Exception):
    pass

class PasswordMismatchError(Exception):
    pass

class TrainingSessionNotFoundError(Exception):
    pass

class ForbiddenError(Exception):
    pass

class LocationIsEmptyError(Exception):
    pass

class CannotCreatePastTrainingSessionError(Exception):
    pass

class TrainingIsPastDueEditWindowError(Exception):
    pass

class InvalidDatesError(Exception):
    pass

class ScheduleWeekdaysMissingError(Exception):
    pass

class TrainingScheduleNotFoundError(Exception):
    pass
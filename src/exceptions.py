class UserNotLoggedInError(Exception):
    pass
class BudgetAlreadyExistsError(Exception):
    pass

class UserNoAccessError(Exception):
    pass
class InvalidDataError(Exception):
    pass

class DatabaseExecutionError(Exception):
    pass


class BudgetDataProcessorError(Exception):
    pass

class MissingRequiredColumnsError(BudgetDataProcessorError):
    pass

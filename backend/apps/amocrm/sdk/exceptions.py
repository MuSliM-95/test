class BaseModuleException(Exception):
    """
    Base exception for all
    """


class AmoApiPageIsEmpty(BaseModuleException):
    """
    Raised when page is empty
    """


class AmoInstallNotFound(BaseModuleException):
    """
    Raised when amo install not found in database
    """


class AmoLinkTableNotFound(BaseModuleException):
    """
    Raised when amo link amo with table not found
    """


class AmoUnAuthorized(BaseModuleException):
    """
    Raised when user is not authorized
    """

class MarketplaceError(Exception):
    pass


class NotPublicError(MarketplaceError):
    pass


class NotFoundError(MarketplaceError):
    pass


class AlreadyInFavoritesError(MarketplaceError):
    pass


class ValidationError(MarketplaceError):
    pass



class NonExistentTableException(Exception):
    """Raised if an operation expected a SQL table to exist, but it does not exist"""

    pass

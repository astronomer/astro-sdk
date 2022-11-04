class NonExistentTableException(Exception):
    """Raised if an operation expected a SQL table to exist, but it does not exist"""


class IllegalLoadToDatabaseException(Exception):
    def __init__(self):  # pragma: no cover
        self.message = (
            "Failing this task because you do not have a custom xcom backend set up. If you use "
            "the default XCOM backend to store large dataframes, this can significantly degrade "
            "Airflow DB performance. Please set up a custom XCOM backend (info here "
            "https://docs.astronomer.io/learn/custom-xcom-backends) or set the environment "
            "variable AIRFLOW__ASTRO_SDK__DATAFRAME_ALLOW_UNSAFE_STORAGE to true if you wish to proceed while "
            "knowing the risks. "
        )
        super().__init__(self.message)


class DatabaseCustomError(ValueError, AttributeError):
    """
    Inappropriate argument value (of correct type) or attribute
    not found while running query. while running query
    """

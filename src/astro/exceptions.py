class NonExistentTableException(Exception):
    """Raised if an operation expected a SQL table to exist, but it does not exist"""

    pass


class IllegalLoadToDatabaseException(Exception):
    def __init__(self):
        self.message = (
            "Failing this task because you do not have a custom xcom backend set up. If you use "
            "the default XCOM backend to store large dataframes, this can significantly degrade "
            "Airflow DB performance. Please set up a custom XCOM backend (info here "
            "https://www.astronomer.io/guides/custom-xcom-backends) or set the environment "
            "variable AIRFLOW__ASTRO_SDK__DATAFRAME_ALLOW_UNSAFE_STORAGE to true if you wish to proceed while "
            "knowing the risks. "
        )
        super().__init__(self.message)

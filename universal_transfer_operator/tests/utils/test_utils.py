import random
import string


def create_unique_str(length: int = 50) -> str:
    """
    Create a unique table name of the requested size, which is compatible with all supported databases.
    :return: Unique table name
    :rtype: str
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(length - 1)
    )
    return unique_id

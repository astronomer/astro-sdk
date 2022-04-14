import random
import string
from dataclasses import dataclass
from typing import List, Optional, Union

from sqlalchemy import Column

MAX_TABLE_NAME_LENGTH = 63


@dataclass
class Metadata:
    schema: Union[str, None] = None
    database: Union[str, None] = None
    warehouse: Union[str, None] = None
    role: Union[str, None] = None


@dataclass
class Table:
    name: Optional[str] = None
    metadata: Optional[Metadata] = None
    columns: Optional[List[Column]] = None
    temp: bool = False

    def __post_init__(self):
        if self.columns is None:
            self.columns = []

        if self.metadata is None:
            self.metadata = Metadata()

        if self.name is None:
            self.name = self._create_unique_table_name()

    def _create_unique_table_name(self):
        unique_id = random.choice(string.ascii_lowercase) + "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in range(MAX_TABLE_NAME_LENGTH - 1)
        )
        return unique_id

    @property
    def qualified_name(self):
        return self.name

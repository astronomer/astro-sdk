from typing import List, Optional

import attr


@attr.define
class LoadOptions:
    def empty(self):
        return NotImplementedError()

    def to_dict(self) -> dict:
        """
        Convert options class to dict
        """
        return attr.asdict(self)


@attr.define
class LoadOptionsList:
    _load_options: List[LoadOptions]

    def get(self, option_type) -> Optional[LoadOptions]:
        for option in self._load_options:
            if type(option).__name__ == option_type:
                return option
        return None

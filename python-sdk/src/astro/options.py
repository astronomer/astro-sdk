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

    def get(self, option_class) -> Optional[LoadOptions]:
        """
        Check `LOAD_OPTIONS_CLASS_NAME` attribute and select the correct load_options
        :param option_class: FileType | FileLocation | BaseDatabase
        """
        if not hasattr(option_class, "LOAD_OPTIONS_CLASS_NAME"):
            return None
        return self.get_by_class_name(option_class.LOAD_OPTIONS_CLASS_NAME)

    def get_by_class_name(self, option_class_name):
        """
        Get load_option by class name
        :return:
        """
        for option in self._load_options:
            if type(option).__name__ == option_class_name:
                return option
        return None

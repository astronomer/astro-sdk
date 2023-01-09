from typing import Dict, List, Optional

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


def list_to_dict(value: Optional[List[LoadOptions]]) -> Optional[Dict[str, LoadOptions]]:
    """
    Convert list object to dict
    """
    if value is None:
        return None
    return {type(option).__name__: option for option in value}


@attr.define
class LoadOptionsList:
    _load_options: Optional[Dict[str, LoadOptions]] = attr.field(converter=list_to_dict)

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
        if self._load_options is None:
            return None
        return self._load_options.get(option_class_name, None)

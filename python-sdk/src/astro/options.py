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

    def get_by_class_name(self, option_class_name) -> Optional[LoadOptions]:
        """
        Get load_option by class name
        :return:
        """
        if self._load_options is None:
            return None
        return self._load_options.get(option_class_name, None)


@attr.define
class SnowflakeLoadOptions(LoadOptions):
    """
    Load options to load file to snowflake using native approach.

    :param file_options: Depending on the file format type specified, use one or more of the
        format-specific options as key-value pair. Read more at:
        https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#format-type-options-formattypeoptions
    :param copy_options: Specify one or more of the copy option as key-value pair. Read more at:
        https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions
    """

    file_options: dict = attr.field(init=True, factory=dict)
    copy_options: dict = attr.field(init=True, factory=dict)

    def empty(self):
        return not self.file_options and not self.copy_options

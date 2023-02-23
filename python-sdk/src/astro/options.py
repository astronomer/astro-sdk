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


def contains_required_option(load_options: Optional[LoadOptions], option_name: str) -> bool:
    """
    Check required options in load_option class
    """
    return bool(load_options and getattr(load_options, option_name, None))


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
        cls = None
        for cls_name in option_class.LOAD_OPTIONS_CLASS_NAME:
            cls = self.get_by_class_name(cls_name)
            if cls is not None:
                break
        return cls

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
    :param storage_integration: Specify the previously created Snowflake storage integration
    :param validation_mode: Defaults to `validation_mode=None`. This instructs the COPY command to validate the data
        files instead of loading them into the specified table; i.e. the COPY command tests the files for errors but
        does not load them. Supported validation mode; `RETURN_n_ROWS` | `RETURN_ERRORS` | `RETURN_ALL_ERRORS`

    .. note::
        Specify the supported validation mode;

        - `RETURN_n_ROWS`: validates the specified n rows if no errors are encountered; otherwise, fails at the first
          error encountered in the rows.
        - `RETURN_ERRORS`: returns all errors (parsing, conversion, etc.) across all files specified in the COPY
          statement.
        - `RETURN_ALL_ERRORS`: returns all errors across all files specified in the `COPY` statement, including files
          with errors that were partially loaded during an earlier load because the `ON_ERROR` copy option was set
          to `CONTINUE` during the load.

        Read more at: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#optional-parameters
    """

    file_options: dict = attr.field(init=True, factory=dict)
    copy_options: dict = attr.field(init=True, factory=dict)
    validation_mode: str = attr.field(default=None)
    storage_integration: str = attr.field(default=None)

    def empty(self):
        return not self.file_options and not self.copy_options


@attr.define
class WASBLocationLoadOptions(LoadOptions):
    storage_account: str = attr.field(default=None)

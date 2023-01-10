import attr


@attr.define
class LoadOptions:
    def empty(self):
        return NotImplementedError()


@attr.define
class SnowflakeLoadOptions(LoadOptions):
    file_options: dict = attr.field(init=True, factory=dict)
    copy_options: dict = attr.field(init=True, factory=dict)

    def empty(self):
        return not self.file_options and not self.copy_options

import attr


@attr.define
class LoadOptions:
    def empty(self):
        return NotImplementedError()

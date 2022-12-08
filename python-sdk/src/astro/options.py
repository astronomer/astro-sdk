from attrs import define


@define
class LoadOptions:
    def empty(self):
        return NotImplementedError()

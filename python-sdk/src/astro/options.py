from __future__ import annotations

import attr


@attr.define
class LoadOptions:
    def empty(self):
        return NotImplementedError()

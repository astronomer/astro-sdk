from astro.constants import FileType as FileTypeConstants
from astro.files.types import ExcelFileType


class XLSFileType(ExcelFileType):
    @property
    def name(self):
        return FileTypeConstants.XLS

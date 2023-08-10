from astro.files.types import ExcelFileType
from astro.constants import FileType as FileTypeConstants


class XLSFileType(ExcelFileType):

    @property
    def name(self):
        return FileTypeConstants.XLS

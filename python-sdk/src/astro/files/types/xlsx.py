from astro.files.types import ExcelFileType
from astro.constants import FileType as FileTypeConstants


class XLSXFileType(ExcelFileType):
    @property
    def name(self):
        return FileTypeConstants.XLSX

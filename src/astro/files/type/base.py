# def get_filetype(filepath: Union[str, pathlib.PosixPath]) -> FileType:
#     """
#     Return a FileType given the filepath. Uses a naive strategy, using the file extension.
#
#     :param filepath: URI or Path to a file
#     :type filepath: str or pathlib.PosixPath
#     :return: The filetype (e.g. csv, ndjson, json, parquet)
#     :rtype: astro.constants.FileType
#     """
#     if isinstance(filepath, pathlib.PosixPath):
#         extension = filepath.suffix[1:]
#     else:
#         extension = filepath.split(".")[-1]
#
#     try:
#         return FileType[extension.upper()]
#     except KeyError:
#         raise ValueError(f"Unsupported filetype '{extension}' from file '{filepath}'.")

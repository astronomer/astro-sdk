# def flatten_ndjson(
#     normalize_config: Union[None, dict], stream: io.TextIOWrapper
# ) -> pd.DataFrame:
#     """
#     Flatten the nested ndjson/json.
#
#     :param normalize_config: parameters in dict format of pandas json_normalize() function.
#         https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html
#     :param stream: io.TextIOWrapper object for the file
#     :type normalize_config: dict
#     :type stream: io.TextIOWrapper
#     :return: return dataframe containing the loaded data
#     :rtype: `pandas.DataFrame`
#     """
#     normalize_config = normalize_config or {}
#
#     df = None
#     rows = stream.readlines(DEFAULT_CHUNK_SIZE)
#     while len(rows) > 0:
#         if df is None:
#             df = pd.DataFrame(
#                 pd.json_normalize([json.loads(row) for row in rows], **normalize_config)
#             )
#         rows = stream.readlines(DEFAULT_CHUNK_SIZE)
#     return df
#
#
# def populate_normalize_config(
#     ndjson_normalize_sep, database: Database
# ) -> Dict[str, str]:
#     """
#     Validate pandas json_normalize() parameter for databases, since default params result in
#     invalid column name. Default parameter result in the columns name containing '.' char.
#
#     :param ndjson_normalize_sep: separator used to normalize nested ndjson.
#         https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html
#     :type ndjson_normalize_sep: str
#     :return: return updated config
#     :rtype: `dict`
#     """
#     normalize_config = {
#         "meta_prefix": ndjson_normalize_sep,
#         "record_prefix": ndjson_normalize_sep,
#         "sep": ndjson_normalize_sep,
#     }
#     replacement = "_"
#     illegal_char = "."
#
#     if database in [Database.BIGQUERY, Database.SNOWFLAKE]:
#         meta_prefix = ndjson_normalize_sep
#         if meta_prefix and meta_prefix == illegal_char:
#             normalize_config["meta_prefix"] = replacement
#
#         record_prefix = normalize_config.get("record_prefix")
#         if record_prefix and record_prefix == illegal_char:
#             normalize_config["record_prefix"] = replacement
#
#         sep = normalize_config.get("sep")
#         if sep is None or sep == illegal_char:
#             normalize_config["sep"] = replacement
#
#     return normalize_config

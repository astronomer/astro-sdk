import unittest

from astro.sql.parsers.sql_directory_parser import parse_directory


class TestSQLParsing(unittest.TestCase):
    def test_parse(self):
        x = parse_directory()
        print(x)

#!/bin/env python3
# coding: utf8


import argparse
import logging
import pandas as pd
import swifter
import math
from decimal import Decimal


class NumberDiscover:
    """Given a list of string, discovers if:
    - They are all numbers, then find the tightest representation
    - Else return "string"
    """

    @staticmethod
    def discover(values):
        def is_number(x):
            """Tests if a string is a valid number

            Args:
                x (string): string to be checked

            Returns:
                True if x is a valid number
                False otherwise
            """
            try:
                float(x)
            except ValueError:
                return False
            else:
                return True

        pre_dot_max_length = 0
        post_dot_max_length = 0

        number_of_nulls = 0

        max_pre = 0
        min_pre = 0

        for v in values:

            # If it is a NULL value (empty), then skip this value
            if len(v) == 0:
                number_of_nulls += 1
                continue
            if not is_number(v):
                return "string"

            if math.isnan(float(v)):
                number_of_nulls += 1
                continue

            # If Scientific Notation, expand it
            if 'e' in v:
                v = format(Decimal(v), 'f')

            # Split the number into two values
            splitted = v.split('.', 1)

            # Preprocess before dot part
            # If the pre dot part is implicit, e.g. .10
            pre_dot_string = splitted[0]
            if pre_dot_string == '':
                pre_dot_string = '0'

            # Use python built-in functions to cast to int
            # which has no size limit
            pre = int(pre_dot_string)
            pre_s = str(pre)

            # Remove signal
            if pre_s[0] == '-' or pre_s[0] == '+':
                pre_s = pre_s[1:]
            pre_number_of_digits = len(pre_s)

            if max_pre < pre:
                max_pre = pre

            if min_pre > pre:
                min_pre = pre

            if pre_number_of_digits > pre_dot_max_length:
                pre_dot_max_length = pre_number_of_digits

            # If there is a post dot part process it
            if len(splitted) > 1 and len(splitted[1]) > 0:
                # Remove tail spaces and zeros
                post_dot_string = splitted[1].rstrip(' ').rstrip('0')

                post_dot_length = len(post_dot_string)

                if post_dot_length > post_dot_max_length:
                    post_dot_max_length = post_dot_length

        if number_of_nulls == len(values):
            return "tinyint"

        # Only found integers
        if post_dot_max_length == 0:
            if -128 < min_pre and max_pre <= 127:
                return "tinyint"
            elif -32768 < min_pre and max_pre <= 32767:
                return "smallint"
            elif -2147483648 < min_pre and max_pre <= 2147483647:
                return "int"
            else:
                return "bigint"

        precision = pre_dot_max_length + post_dot_max_length
        scale = post_dot_max_length
        return f"""decimal({precision}, {scale})"""


class SchemaDiscovery:
    """Given a *correct* CSV file, discovers the best fitting tight schema.

    Args:
        table_name (str): table_name to use on the SQL script.abs
        file_path (str): path to csv file (full or relative path).
        skip_lines (uint): number of lines to skip from start of CSV.
                           Default = 0.
        separator (str): attribute separator for CSV. Default = ','.
        null (str): string that represents a NULL value.
    """

    def __init__(
        self,
        table_name,
        file_path,
        skip_lines=0,
        separator=',',
        null=''
    ):
        """Please see help(SchemaDiscovery) for more info"""
        self.file_path = file_path
        self.skip_lines = skip_lines
        self.separator = separator
        self.table_name = table_name
        self.null = null

    def run(self):
        """Discovers the schema.

        Returns:
            str: returns the SQL string to create the table using the schema.
        """
        logging.info('!@@ Started SchemaDiscovery @@!')

        logging.info('Reading CSV')
        df = self.__read_csv()

        logging.info('Starting type discovery')
        types = self.__discover_schema(df)

        logging.info('Translating to SQL create table statement')
        return self.__to_sql(types)

    def __read_csv(self):
        """Reads the CSV file and stores it in a Pandas DataFrame

        Returns:
            DataFrame: pandas dataframe
        """
        df = pd.read_csv(
                self.file_path,
                dtype=str,
                sep=self.separator,
                skiprows=self.skip_lines,
                index_col=False,
                na_values=self.null,
                na_filter=False,
                engine="c")
        return df

    def __discover_schema(self, df):
        """Given a DataFrame, discovers the best fitting
            types for each column.

        Args:
            df: pandas DataFrame

        Returns:
            Dictionary with keys as column names, and
                values as types
        """
        types = {}

        types = df.swifter.apply(NumberDiscover.discover, axis=0, raw=True)

        return types

    def __to_sql(self, types):
        """Translates the dictionary into a SQL create table.

        Example:
            Dictionary:{
               col1: string,
               col2: integer,
               col3: real
            }
            Returns:
                CREATE TABLE <table_name> (
                    "col1" string,
                    "col2" integer,
                    "col3" real
                );

        Args:
            types (dictionary): dictionary with column names as keys
                and types as values.

        Returns:
            str: sql create table statement.
        """

        if len(types) < 1:
            logging.error(
                "Size of dictionary equals 0, \
                 no attributes to create SQL query"
            )
            raise KeyError

        sql = f"""CREATE TABLE \"{self.table_name}\" ("""

        for col, t in types.items():
            sql += f"""\n\t"{col}" {t},"""

        sql = sql[:-1]  # remove last comma
        sql += "\n);"

        return sql


def main():
    # ----- Logging Info -----
    logging.basicConfig(
        filename='/tmp/schema_discovery.log',
        filemode='a',
        level=logging.INFO
    )
    logging.Formatter(
        "%(asctime)s| %(name)s - %(levelname)s - %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )
    # ----- Logging Info -----

    parser = argparse.ArgumentParser(
        description="""Determine SQL schema from CSV data."""
    )

    parser.add_argument('files', metavar='FILES', nargs='+',
                        help='CSV files to process.')
    parser.add_argument("-S", "--sep", dest="separator",
                        help="Use <SEPERATOR> as delimiter between columns",
                        default=",")
    parser.add_argument("-B", "--begin", type=int, dest="begin",
                        help="Skips first <BEGIN> rows", default="0")
    parser.add_argument("-N", "--null", dest="null", type=str,
                        help="Interprets <NULL> as NULLs", default="")
    args = parser.parse_args()

    for f in args.files:
        discoverer = SchemaDiscovery(
            "test",
            f,
            args.begin,
            args.separator,
            args.null
        )
        print(discoverer.run())


if __name__ == '__main__':
    main()
